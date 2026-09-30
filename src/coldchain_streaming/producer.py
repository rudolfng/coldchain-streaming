"""Replay determinista del tramo elegido a Kafka, con inyecciones.

El calendario separa el tiempo de evento (nunca se acelera, conserva la separación real
entre lecturas) del reloj de pared (comprimido por `speedup`). Las inyecciones deterministas
(semilla fija) retienen y liberan en ráfaga los eventos de un sensor, reenvían un bloque de
otro sensor y corrompen una fracción de eventos; todo se registra en el log con `event_id`
y motivo para que la corrida sea auditable (README sección 4).
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import math
import random
import signal
import time
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event

import pandas as pd
from confluent_kafka import Producer

from coldchain_streaming.config import Settings, processed_dataset_path
from coldchain_streaming.contracts import build_event, encode_event

logger = logging.getLogger("coldchain_streaming.producer")

# (inicio_retencion_s, fin_retencion_s, liberacion_s) por sensor, elapsed_s del dataset.
RETENTION_WINDOWS: dict[str, dict[str, tuple[float, float, float]]] = {
    "demo": {
        "ULT1-b07": (66 * 3600 + 20 * 60, 66 * 3600 + 26 * 60, 66 * 3600 + 26 * 60),
        "ULT1-b12": (66 * 3600 + 40 * 60, 66 * 3600 + 45 * 60, 67 * 3600 + 5 * 60),
    },
    "smoke": {
        "ULT1-b07": (65 * 3600 + 5 * 60, 65 * 3600 + 8 * 60, 65 * 3600 + 10 * 60),
    },
    "none": {},
}
# (sensor, momento de la ráfaga en elapsed_s, cantidad de eventos previos a reenviar).
BURST_RESEND: dict[str, tuple[str, float, int] | None] = {
    "demo": ("ULT1-b03", 66 * 3600 + 50 * 60, 20),
    "smoke": ("ULT1-b03", 65 * 3600 + 12 * 60, 5),
    "none": None,
}


@dataclass(frozen=True)
class PlannedItem:
    """Un mensaje a emitir: normal, duplicado o reenvío en ráfaga."""

    kind: str  # "normal" | "duplicate" | "burst_resend"
    sensor_id: str
    source_time: datetime
    offset_s: float  # usado para event_time (tiempo de evento, sin acelerar)
    emit_offset_s: float  # usado para el reloj de pared (se divide por speedup)
    temp_f: float | None
    temp_c: float | None
    invalid_mode: str | None  # None | "remove_temp_c" | "temp_c_text"


def load_events(path: Path | None = None) -> pd.DataFrame:
    frame = pd.read_parquet(path or processed_dataset_path())
    return frame.sort_values(["elapsed_s", "sensor_id"], kind="stable").reset_index(drop=True)


def plan_replay(
    events: pd.DataFrame,
    *,
    seed: int = 7,
    injections: str = "demo",
    duplicate_rate: float = 0.02,
    invalid_rate: float = 0.005,
) -> list[PlannedItem]:
    """Construye la secuencia de emisión de forma determinista (sin tocar Kafka ni el reloj)."""
    if events.empty:
        return []
    elapsed_min = float(events["elapsed_s"].min())
    retention = RETENTION_WINDOWS.get(injections, {})
    burst_cfg = BURST_RESEND.get(injections)
    rng = random.Random(seed)

    normal_items: list[PlannedItem] = []
    for row in events.itertuples(index=False):
        window = retention.get(row.sensor_id)
        emit_elapsed = float(row.elapsed_s)
        if window is not None and window[0] <= row.elapsed_s < window[1]:
            emit_elapsed = window[2]
        invalid_mode = None
        if rng.random() < invalid_rate:
            invalid_mode = "remove_temp_c" if rng.random() < 0.5 else "temp_c_text"
        normal_items.append(
            PlannedItem(
                kind="normal",
                sensor_id=row.sensor_id,
                source_time=row.source_time.to_pydatetime(),
                offset_s=float(row.elapsed_s) - elapsed_min,
                emit_offset_s=emit_elapsed - elapsed_min,
                temp_f=None if pd.isna(row.temp_f) else float(row.temp_f),
                temp_c=None if pd.isna(row.temp_c) else float(row.temp_c),
                invalid_mode=invalid_mode,
            )
        )

    duplicate_items = [
        dataclasses.replace(item, kind="duplicate")
        for item in normal_items
        if rng.random() < duplicate_rate
    ]

    burst_items: list[PlannedItem] = []
    if burst_cfg is not None:
        burst_sensor, burst_at_h, burst_count = burst_cfg
        burst_at_s = burst_at_h
        candidates = sorted(
            (
                item
                for item in normal_items
                if item.sensor_id == burst_sensor and (item.offset_s + elapsed_min) < burst_at_s
            ),
            key=lambda item: item.offset_s,
        )
        burst_items = [
            dataclasses.replace(item, kind="burst_resend", emit_offset_s=burst_at_s - elapsed_min)
            for item in candidates[-burst_count:]
        ]

    combined = normal_items + duplicate_items + burst_items
    combined.sort(key=lambda item: (item.emit_offset_s, item.offset_s, item.kind != "normal"))
    return combined


def build_raw_bytes(item: PlannedItem, *, t0_event: datetime) -> tuple[bytes, str, datetime]:
    """Codifica un `PlannedItem` a JSON canónico, aplicando la corrupción si corresponde."""
    event_time = t0_event + timedelta(seconds=item.offset_s)
    event = build_event(
        sensor_id=item.sensor_id,
        source_time=item.source_time,
        event_time=event_time,
        temp_f=item.temp_f,
        temp_c=item.temp_c,
    )
    payload = event.as_dict()
    if item.invalid_mode == "remove_temp_c":
        del payload["payload"]["temp_c"]
    elif item.invalid_mode == "temp_c_text":
        payload["payload"]["temp_c"] = "n/a"
    return encode_event(payload), event.event_id, event_time


def aligned_start_delay(now_epoch: float, align_seconds: float, lead_seconds: float) -> float:
    """Segundos a esperar para que los minutos redondos del ensayo caigan en bordes de ventana.

    `lead_seconds` es el `elapsed_s` de la primera muestra del tramo; solo importa su resto
    módulo la ventana. Arrancando en `borde + lead`, un instante del ensayo múltiplo de la
    ventana (p. ej. 66:40:00, inicio de la retención de b12) cae justo en un borde, sin importar
    `REPLAY_START_HOURS`, y cada inyección ocupa ventanas completas (demo en tiempo real).
    """
    lead = lead_seconds % align_seconds
    target = math.ceil((now_epoch - lead) / align_seconds) * align_seconds + lead
    return target - now_epoch


def build_producer(bootstrap_servers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "coldchain-replay-producer",
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "snappy",
        }
    )


class ColdchainReplay:
    """Reproduce `PlannedItem`s a un tópico de Kafka respetando el calendario."""

    def __init__(self, producer: Producer, *, topic: str, speedup: float = 20.0) -> None:
        if speedup <= 0:
            raise ValueError("speedup must be positive")
        self.producer = producer
        self.topic = topic
        self.speedup = speedup
        self.stop_event = Event()
        self.partitions: Counter[int] = Counter()
        self.counts: Counter[str] = Counter()
        self.produced_count = 0
        self.delivery_errors = 0
        self.undelivered = 0
        self.released_late: list[dict[str, object]] = []

    def stop(self) -> None:
        self.stop_event.set()

    def _on_delivery(self, error, message) -> None:
        if error is None:
            self.partitions[message.partition()] += 1
            return
        # Sin esto, un mensaje que Kafka no aceptó figuraría como enviado en el resumen.
        self.delivery_errors += 1
        logger.warning("entrega fallida topico=%s error=%s", message.topic(), error)

    def _produce(self, item: PlannedItem, *, t0_event: datetime) -> None:
        raw, event_id, event_time = build_raw_bytes(item, t0_event=t0_event)
        timestamp_ms = int(event_time.timestamp() * 1000)
        self.producer.produce(
            self.topic,
            key=item.sensor_id.encode(),
            value=raw,
            timestamp=timestamp_ms,
            on_delivery=self._on_delivery,
        )
        self.producer.poll(0)
        self.produced_count += 1
        self.counts[item.kind] += 1
        if item.invalid_mode is not None:
            self.counts["invalid"] += 1
            logger.info(
                "invalido inyectado sensor=%s event_id=%s motivo=%s",
                item.sensor_id,
                event_id,
                item.invalid_mode,
            )
        if item.kind == "duplicate":
            logger.info("duplicado sensor=%s event_id=%s", item.sensor_id, event_id)
        if item.kind == "burst_resend":
            logger.info(
                "reenvio en rafaga sensor=%s event_id=%s emit_elapsed_s=%.0f",
                item.sensor_id,
                event_id,
                item.emit_offset_s,
            )
        if item.kind == "normal" and item.emit_offset_s != item.offset_s:
            record = {
                "sensor_id": item.sensor_id,
                "event_id": event_id,
                "original_elapsed_s": item.offset_s,
                "released_elapsed_s": item.emit_offset_s,
            }
            self.released_late.append(record)
            logger.info(
                "retenido y liberado sensor=%s event_id=%s original_elapsed_s=%.0f "
                "liberado_elapsed_s=%.0f",
                item.sensor_id,
                event_id,
                item.offset_s,
                item.emit_offset_s,
            )

    def replay(self, items: list[PlannedItem], *, realtime: bool = True) -> dict[str, object]:
        if not items:
            return {"events": 0, **self._summary()}
        # El tramo empieza "ahora" y su event_time se adelanta al reloj. Anclarlo al pasado
        # (que termine "ahora") es peor: CreateTime salta el watermark al reloj y todo el
        # tramo llega fuera de allowed_lateness y se descarta.
        t0_event = datetime.now(UTC)
        wall0 = time.monotonic()
        logger.info(
            "inicio del replay t0_event=%s speedup=%s mensajes=%d",
            t0_event.isoformat(),
            self.speedup,
            len(items),
        )
        for item in items:
            if self.stop_event.is_set():
                break
            due = wall0 + item.emit_offset_s / self.speedup
            if realtime:
                while not self.stop_event.is_set() and (remaining := due - time.monotonic()) > 0:
                    time.sleep(min(remaining, 0.1))
            self._produce(item, t0_event=t0_event)
        # flush devuelve cuántos mensajes quedaron sin confirmar al vencer el plazo.
        self.undelivered = self.producer.flush(30)
        if self.undelivered:
            logger.warning("%d mensajes sin confirmar al terminar el replay", self.undelivered)
        return {"events": self.produced_count, **self._summary()}

    def _summary(self) -> dict[str, object]:
        return {
            "by_kind": dict(self.counts),
            "invalid": self.counts.get("invalid", 0),
            "duplicates": self.counts.get("duplicate", 0),
            "burst_resends": self.counts.get("burst_resend", 0),
            "released_late": self.released_late,
            "partitions": dict(self.partitions),
            # delivered = confirmados por Kafka; events cuenta los que se intentó enviar.
            "delivered": sum(self.partitions.values()),
            "delivery_errors": self.delivery_errors,
            "undelivered": self.undelivered,
        }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_env()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, default=processed_dataset_path())
    parser.add_argument("--speedup", type=float, default=settings.replay_speedup)
    parser.add_argument("--seed", type=int, default=settings.replay_seed)
    parser.add_argument(
        "--injections", choices=sorted(RETENTION_WINDOWS), default=settings.replay_injections
    )
    parser.add_argument("--duplicate-rate", type=float, default=settings.replay_duplicate_rate)
    parser.add_argument("--invalid-rate", type=float, default=settings.replay_invalid_rate)
    parser.add_argument("--no-realtime", action="store_true")
    parser.add_argument(
        "--align-to-window",
        action=argparse.BooleanOptionalAction,
        default=settings.replay_align_start,
        help="esperar a que el inicio del tramo coincida con un borde de ventana",
    )
    args = parser.parse_args()

    events = load_events(args.dataset)
    items = plan_replay(
        events,
        seed=args.seed,
        injections=args.injections,
        duplicate_rate=args.duplicate_rate,
        invalid_rate=args.invalid_rate,
    )
    replay = ColdchainReplay(
        build_producer(settings.kafka_bootstrap_servers),
        topic=settings.topic_raw,
        speedup=args.speedup,
    )
    signal.signal(signal.SIGTERM, lambda *_: replay.stop())
    signal.signal(signal.SIGINT, lambda *_: replay.stop())
    if args.align_to_window and not args.no_realtime and not events.empty:
        lead = float(events["elapsed_s"].min())
        delay = aligned_start_delay(time.time(), settings.window_seconds, lead)
        logger.info("esperando %.1f s para alinear el inicio del tramo con una ventana", delay)
        replay.stop_event.wait(delay)
    result = replay.replay(items, realtime=not args.no_realtime)
    print(json.dumps(result, default=str, indent=2))


if __name__ == "__main__":
    main()
