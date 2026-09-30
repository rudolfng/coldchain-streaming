"""Transforms de Beam: parseo, agregación incremental (con dedup) y panes.

Validar (con DLQ), asignar tiempo de evento, ventanear, combinar por sensor con
`CombinePerKey` (deduplicando por `event_id` dentro del combinador, ver `TempStatsCombineFn`)
y dar formato al pane. La lectura y la escritura en Kafka están en `pipeline.py`.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.metrics import Metrics
from apache_beam.transforms import trigger, window
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.windowed_value import PaneInfoTiming

from coldchain_streaming.config import ASSET_ID, TEMP_MAX_C, TEMP_MIN_C, Settings
from coldchain_streaming.contracts import ValidationResult, validate_event


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _best_effort_event_id(raw: bytes) -> str | None:
    """Intenta rescatar el `event_id` de un mensaje que no pasó la validación completa.

    Corre fuera de la red de seguridad de `ParseAndValidate`, así que no puede lanzar nada:
    un rescate que falla devuelve None (p. ej. RecursionError con un JSON muy anidado).
    """
    try:
        decoded = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
    except Exception:  # noqa: BLE001
        return None
    if isinstance(decoded, dict) and isinstance(decoded.get("event_id"), str):
        return decoded["event_id"]
    return None


METRICS_NAMESPACE = "coldchain"


class ParseAndValidate(beam.DoFn):
    """Decodifica y valida cada mensaje; lo inválido va a la salida lateral.

    Cuenta con contadores de Beam (`coldchain/events_valid` y `coldchain/invalid_<motivo>`),
    visibles en la UI de Flink junto a los registros por paso.
    """

    INVALID = "invalid"

    def __init__(self) -> None:
        super().__init__()
        self.valid_counter = Metrics.counter(METRICS_NAMESPACE, "events_valid")

    def process(self, element: tuple[bytes, bytes]):
        _key, raw = element
        try:
            result = validate_event(raw)
        except Exception as error:  # noqa: BLE001
            # Red de seguridad: una excepción acá fallaría el bundle y Flink releería el mismo
            # mensaje desde el checkpoint en bucle. Lo que el validador no prevea va a la DLQ.
            result = ValidationResult(None, f"unexpected:{type(error).__name__}", None)
        if result.is_valid:
            self.valid_counter.inc()
            yield result.event
            return
        # El motivo va sin sufijos (`missing_field:x` -> `invalid_missing_field`) para no crear
        # un contador por cada campo o excepción posible.
        reason_kind = (result.reason or "unknown").split(":", 1)[0]
        Metrics.counter(METRICS_NAMESPACE, f"invalid_{reason_kind}").inc()
        yield pvalue.TaggedOutput(
            self.INVALID,
            {
                "reason": result.reason,
                "event_id": _best_effort_event_id(raw),
                "sensor_id": result.sensor_id or "unknown",
                "raw": raw.decode(errors="replace") if isinstance(raw, bytes) else raw,
                "observed_at": _iso(datetime.now(UTC)),
            },
        )


def assign_event_timestamp(event: dict[str, Any]) -> TimestampedValue:
    """Usa `event_time` (UTC) como timestamp del elemento: desde acá todo es tiempo de evento."""
    parsed = datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        # Igual que el validador: sin zona es UTC, no la hora local del worker.
        parsed = parsed.replace(tzinfo=UTC)
    return TimestampedValue(event, parsed.timestamp())


def windowed(events, settings: Settings, *, streaming_triggers: bool = True):
    kwargs: dict[str, Any] = {
        "windowfn": window.FixedWindows(settings.window_seconds),
        "allowed_lateness": settings.allowed_lateness_seconds,
        "accumulation_mode": trigger.AccumulationMode.ACCUMULATING,
    }
    if streaming_triggers:
        kwargs["trigger"] = trigger.AfterWatermark(
            early=trigger.AfterProcessingTime(settings.early_firing_seconds),
            late=trigger.AfterCount(1),
        )
    return events | "Fixed 5-min windows" >> beam.WindowInto(**kwargs)


class TempStatsCombineFn(beam.CombineFn):
    """Acumulador `{event_id: temp_c}`: estadísticas incrementales con dedup incorporado.

    No se deduplica en una etapa previa (`GroupByKey` por `event_id`): con disparos
    `early`/`late` en modo ACCUMULATING esa etapa reenvía su conjunto completo en cada disparo
    y el combinador lo vuelve a sumar. Acá `add_input` es idempotente (mismo `event_id`, misma
    clave) y `extract_output` recalcula n/avg/min/max en cada disparo. El horizonte de la
    deduplicación es ventana + lateness: el acumulador vive lo que vive la ventana.
    """

    def create_accumulator(self) -> dict[str, float]:
        return {}

    def add_input(self, accumulator: dict[str, float], event: dict[str, Any]) -> dict[str, float]:
        accumulator = dict(accumulator)
        accumulator[event["event_id"]] = float(event["payload"]["temp_c"])
        return accumulator

    def merge_accumulators(self, accumulators) -> dict[str, float]:
        merged: dict[str, float] = {}
        for accumulator in accumulators:
            merged.update(accumulator)
        return merged

    def extract_output(self, accumulator: dict[str, float]) -> dict[str, Any]:
        if not accumulator:
            return {
                "n_readings": 0,
                "avg_temp_c": None,
                "min_temp_c": None,
                "max_temp_c": None,
                "out_of_range": False,
            }
        values = list(accumulator.values())
        low, high = min(values), max(values)
        return {
            "n_readings": len(values),
            "avg_temp_c": round(sum(values) / len(values), 2),
            "min_temp_c": round(low, 2),
            "max_temp_c": round(high, 2),
            "out_of_range": high > TEMP_MAX_C or low < TEMP_MIN_C,
        }


class FormatPane(beam.DoFn):
    """Agrega metadatos de ventana/pane y arma la clave estable de salida."""

    def process(
        self,
        element,
        window_param=beam.DoFn.WindowParam,
        pane_info=beam.DoFn.PaneInfoParam,
    ):
        sensor_id, metrics = element
        window_start = _iso(window_param.start.to_utc_datetime())
        window_end = _iso(window_param.end.to_utc_datetime())
        yield {
            "schema_version": 1,
            "aggregate_id": f"{sensor_id}|{window_start}",
            "sensor_id": sensor_id,
            "asset_id": ASSET_ID,
            "window_start": window_start,
            "window_end": window_end,
            "pane_timing": PaneInfoTiming.to_string(pane_info.timing),
            "pane_index": pane_info.index,
            "is_final": bool(pane_info.is_last),
            "emitted_at": _iso(datetime.now(UTC)),
            **metrics,
        }


def is_non_empty_pane(metric: dict[str, Any]) -> bool:
    return metric["n_readings"] > 0


def build_metrics(events, settings: Settings, *, streaming_triggers: bool = True):
    """Tiempo de evento -> ventana -> combine por sensor (dedup) -> formato -> sin panes vacíos."""
    timestamped = events | "Assign event time" >> beam.Map(assign_event_timestamp)
    windowed_events = windowed(timestamped, settings, streaming_triggers=streaming_triggers)
    keyed = windowed_events | "Key by sensor" >> beam.Map(lambda event: (event["sensor_id"], event))
    aggregated = keyed | "Combine temperature stats" >> beam.CombinePerKey(TempStatsCombineFn())
    formatted = aggregated | "Format panes" >> beam.ParDo(FormatPane())
    # Flink emite un pane final con el acumulador vacío al cerrar una lectura acotada; como el
    # sink aplica el pane_index mayor, ese pane (n_readings=0) pisaba los datos reales. En modo
    # ACCUMULATING un pane legítimo nunca tiene menos lecturas que uno anterior de su ventana.
    return formatted | "Drop empty panes" >> beam.Filter(is_non_empty_pane)


def metric_to_kafka_record(metric: dict[str, Any]) -> tuple[bytes, bytes]:
    return (
        metric["aggregate_id"].encode(),
        json.dumps(metric, sort_keys=True, separators=(",", ":")).encode(),
    )


def dlq_to_kafka_record(record: dict[str, Any]) -> tuple[bytes, bytes]:
    key = record.get("sensor_id") or "unknown"
    return (
        str(key).encode(),
        json.dumps(record, sort_keys=True, separators=(",", ":")).encode(),
    )
