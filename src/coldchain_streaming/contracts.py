"""Contrato de eventos de telemetría (v1/v2) y validación."""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

SENSOR_ID_RE = re.compile(r"^ULT1-b(0[1-9]|1[0-9]|20)$")
SUPPORTED_SCHEMA_VERSIONS = {1, 2}
TEMP_PHYSICAL_MIN_C = -120.0
TEMP_PHYSICAL_MAX_C = 60.0
# El replay acelerado adelanta event_time al reloj hasta duración_tramo * (1 - 1/speedup):
# ~2 h 51 min con el tramo y el speedup por defecto (3 h, 20x). La tolerancia de 4 h lo cubre;
# un tramo de más de ~4 h 12 min a 20x la superaría y caería en la DLQ como stale_clock.
FUTURE_TOLERANCE = timedelta(hours=4)
PAST_TOLERANCE = timedelta(hours=24)

REQUIRED_TOP_FIELDS = (
    "schema_version",
    "event_id",
    "event_time",
    "sensor_id",
    "asset_id",
    "asset_type",
    "payload",
)


def pod_to_sensor_id(pod_label: str) -> str:
    """Convierte 'Pod7' (alias del CSV) en 'ULT1-b07'."""
    number = int(pod_label.removeprefix("Pod"))
    return f"ULT1-b{number:02d}"


def temp_f_to_c(temp_f: float) -> float:
    """Conversión declarada en el adaptador de origen (unidad del CSV: F)."""
    return round((temp_f - 32.0) * 5.0 / 9.0, 2)


def _compact_utc(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def iso_utc(value: datetime | str) -> str:
    parsed = value if isinstance(value, datetime) else _parse_iso(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def event_id_for(sensor_id: str, source_time: datetime | str) -> str:
    """Id determinístico a partir solo de datos de origen: mismo dato -> mismo id."""
    parsed = source_time if isinstance(source_time, datetime) else _parse_iso(str(source_time))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return f"{sensor_id}@{_compact_utc(parsed)}"


@dataclass(frozen=True)
class TelemetryEvent:
    """Evento de `coldchain.telemetry.raw.v1`, contrato v2 (README sección 6)."""

    schema_version: int
    event_id: str
    event_time: str
    sensor_id: str
    asset_id: str
    asset_type: str
    payload: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "event_time": self.event_time,
            "sensor_id": self.sensor_id,
            "asset_id": self.asset_id,
            "asset_type": self.asset_type,
            "payload": self.payload,
        }


def build_event(
    *,
    sensor_id: str,
    source_time: datetime,
    event_time: datetime,
    temp_f: float | None,
    temp_c: float | None,
    asset_id: str = "ULT1",
    asset_type: str = "contenedor",
) -> TelemetryEvent:
    """Construye un evento v2 desde una fila ya recortada del dataset."""
    return TelemetryEvent(
        schema_version=2,
        event_id=event_id_for(sensor_id, source_time),
        event_time=iso_utc(event_time),
        sensor_id=sensor_id,
        asset_id=asset_id,
        asset_type=asset_type,
        payload={
            "temp_c": temp_c,
            "temp_f": temp_f,
            "source_time": iso_utc(source_time),
        },
    )


def encode_event(event: TelemetryEvent | dict[str, Any]) -> bytes:
    """JSON canónico: claves ordenadas, sin espacios, para que event_id sea estable."""
    payload = event.as_dict() if isinstance(event, TelemetryEvent) else event
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


@dataclass(frozen=True)
class ValidationResult:
    """Resultado de `validate_event`: o hay evento válido, o hay un motivo de DLQ."""

    event: dict[str, Any] | None
    reason: str | None
    sensor_id: str | None

    @property
    def is_valid(self) -> bool:
        return self.event is not None


def validate_event(raw: bytes | str, *, now: datetime | None = None) -> ValidationResult:
    """Valida un mensaje crudo campo a campo.

    Motivos posibles (van tal cual a `coldchain.telemetry.dlq.v1`): invalid_json,
    missing_field:<nombre>, unsupported_schema_version, bad_timestamp, stale_clock,
    temp_missing, temp_not_numeric, temp_out_of_physical_range, bad_sensor_id.
    `ParseAndValidate` agrega `unexpected:<excepcion>` si esta función lanza.
    """
    now = now or datetime.now(UTC)
    try:
        text = raw.decode() if isinstance(raw, bytes) else raw
        decoded = json.loads(text)
    except (UnicodeDecodeError, ValueError, TypeError, RecursionError):
        # ValueError incluye JSONDecodeError y el entero de más de 4300 dígitos que Python se
        # niega a convertir; TypeError: mensaje de Kafka sin valor (tombstone, raw=None);
        # RecursionError: JSON anidado miles de niveles.
        return ValidationResult(None, "invalid_json", None)
    if not isinstance(decoded, dict):
        return ValidationResult(None, "invalid_json", None)

    sensor_id = decoded.get("sensor_id") if isinstance(decoded.get("sensor_id"), str) else None

    for name in REQUIRED_TOP_FIELDS:
        if name not in decoded:
            return ValidationResult(None, f"missing_field:{name}", sensor_id)

    if not isinstance(decoded["event_id"], str) or not decoded["event_id"]:
        return ValidationResult(None, "missing_field:event_id", sensor_id)

    schema_version = decoded["schema_version"]
    # Tipo antes que pertenencia: una lista o un dict no son hashables (`in` lanzaría
    # TypeError) y `true` pasaría por 1.
    if (
        isinstance(schema_version, bool)
        or not isinstance(schema_version, int)
        or schema_version not in SUPPORTED_SCHEMA_VERSIONS
    ):
        return ValidationResult(None, "unsupported_schema_version", sensor_id)

    # fullmatch: con match, el `$` del patrón aceptaría "ULT1-b07" seguido de un salto de
    # línea, y abriría una clave aparte.
    if not isinstance(sensor_id, str) or not SENSOR_ID_RE.fullmatch(sensor_id):
        return ValidationResult(None, "bad_sensor_id", sensor_id)

    # Solo texto ISO-8601: un número pasaría por str() y se parsearía (20260925 -> una fecha),
    # pero después `assign_event_timestamp` lo recibiría sin `.replace` y tumbaría el job.
    if not isinstance(decoded["event_time"], str):
        return ValidationResult(None, "bad_timestamp", sensor_id)
    try:
        event_time = _parse_iso(decoded["event_time"])
    except ValueError:
        return ValidationResult(None, "bad_timestamp", sensor_id)
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=UTC)
    if event_time > now + FUTURE_TOLERANCE or event_time < now - PAST_TOLERANCE:
        return ValidationResult(None, "stale_clock", sensor_id)

    payload = decoded["payload"]
    if not isinstance(payload, dict):
        return ValidationResult(None, "missing_field:payload", sensor_id)
    if "temp_c" not in payload:
        return ValidationResult(None, "missing_field:payload.temp_c", sensor_id)

    temp_c = payload["temp_c"]
    if temp_c is None:
        return ValidationResult(None, "temp_missing", sensor_id)
    if isinstance(temp_c, bool) or not isinstance(temp_c, int | float):
        return ValidationResult(None, "temp_not_numeric", sensor_id)
    try:
        finite = math.isfinite(temp_c)
    except OverflowError:
        finite = False
    if not finite or not (TEMP_PHYSICAL_MIN_C <= temp_c <= TEMP_PHYSICAL_MAX_C):
        return ValidationResult(None, "temp_out_of_physical_range", sensor_id)

    return ValidationResult(decoded, None, sensor_id)
