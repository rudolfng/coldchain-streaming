"""Validación campo a campo, conversión F->C, event_id determinista."""

from __future__ import annotations

from datetime import UTC, datetime

from coldchain_streaming.contracts import (
    build_event,
    encode_event,
    event_id_for,
    pod_to_sensor_id,
    temp_f_to_c,
    validate_event,
)

NOW = datetime(2020, 12, 19, 4, 30, 12, tzinfo=UTC)


def valid_raw_event() -> dict:
    event = build_event(
        sensor_id="ULT1-b07",
        source_time=datetime(2020, 12, 19, 4, 30, 12, tzinfo=UTC),
        event_time=NOW,
        temp_f=-113.13,
        temp_c=-80.63,
    )
    return event.as_dict()


def test_pod_to_sensor_id_pads_with_zero():
    """El alias del CSV se traduce al sensor_id del contrato."""
    assert pod_to_sensor_id("Pod9") == "ULT1-b09"
    assert pod_to_sensor_id("Pod13") == "ULT1-b13"
    assert pod_to_sensor_id("Pod20") == "ULT1-b20"


def test_temp_f_to_c_matches_known_conversion():
    """Conversión F->C del adaptador de origen."""
    assert temp_f_to_c(-113.0) == -80.56


def test_event_id_is_deterministic_for_same_source_data():
    """Mismo dato de origen, mismo event_id (base de la dedup)."""
    first = event_id_for("ULT1-b07", datetime(2020, 12, 19, 4, 30, 12, tzinfo=UTC))
    second = event_id_for("ULT1-b07", "2020-12-19T04:30:12Z")
    assert first == second == "ULT1-b07@20201219T043012Z"


def test_valid_v2_event_is_accepted():
    """Un evento v2 bien formado pasa la validación."""
    result = validate_event(encode_event(valid_raw_event()), now=NOW)
    assert result.is_valid
    assert result.reason is None
    assert result.sensor_id == "ULT1-b07"


def test_v1_event_is_accepted_for_backwards_compatibility():
    """Compatibilidad hacia atrás del contrato versionado."""
    event = valid_raw_event()
    event["schema_version"] = 1
    event["payload"].update(humidity_pct=61, battery_pct=82, compressor_on=True)
    result = validate_event(encode_event(event), now=NOW)
    assert result.is_valid


def test_v3_schema_is_rejected():
    """Una versión no soportada va a la DLQ."""
    event = valid_raw_event()
    event["schema_version"] = 3
    result = validate_event(encode_event(event), now=NOW)
    assert result.reason == "unsupported_schema_version"


def test_unhashable_or_boolean_schema_version_goes_to_dlq_instead_of_raising():
    """Una lista o un dict no hashables no tumban el job; `true` no pasa por v1."""
    for bad_version in ([2], {"v": 2}, True):
        event = valid_raw_event()
        event["schema_version"] = bad_version
        result = validate_event(encode_event(event), now=NOW)
        assert result.reason == "unsupported_schema_version"


def test_huge_integer_temperature_goes_to_dlq_instead_of_raising():
    """10**400 no entra en un float (OverflowError); va a la DLQ por rango."""
    event = valid_raw_event()
    event["payload"]["temp_c"] = 10**400
    result = validate_event(encode_event(event), now=NOW)
    assert result.reason == "temp_out_of_physical_range"


def test_integer_beyond_python_digit_limit_is_invalid_json():
    """Más de 4300 dígitos hace que json.loads lance ValueError, no JSONDecodeError."""
    raw = b'{"schema_version":2,"n":' + b"9" * 5000 + b"}"
    result = validate_event(raw, now=NOW)
    assert result.reason == "invalid_json"


def test_deeply_nested_json_is_invalid_json_instead_of_raising():
    """5000 niveles de anidamiento hacen que json.loads lance RecursionError."""
    result = validate_event(b"[" * 5000 + b"]" * 5000, now=NOW)
    assert result.reason == "invalid_json"


def test_sensor_id_with_trailing_newline_is_rejected():
    """Un sensor_id "ULT1-b07" con salto de línea final no abre una clave aparte."""
    event = valid_raw_event()
    event["sensor_id"] = "ULT1-b07\n"
    result = validate_event(encode_event(event), now=NOW)
    assert result.reason == "bad_sensor_id"


def test_invalid_json_is_rejected():
    """Un JSON roto va a la DLQ con motivo."""
    result = validate_event(b"{not json", now=NOW)
    assert result.reason == "invalid_json"
    assert result.sensor_id is None


def test_message_without_value_goes_to_dlq_instead_of_raising():
    """Un tombstone de Kafka (valor nulo) no tumba el job."""
    result = validate_event(None, now=NOW)  # tombstone de Kafka: valor nulo
    assert result.reason == "invalid_json"


def test_missing_field_is_reported_by_name():
    """El motivo de DLQ nombra el campo faltante."""
    event = valid_raw_event()
    del event["asset_id"]
    result = validate_event(encode_event(event), now=NOW)
    assert result.reason == "missing_field:asset_id"


def test_bad_sensor_id_is_rejected():
    """Un sensor ajeno al contenedor va a la DLQ."""
    event = valid_raw_event()
    event["sensor_id"] = "ULT1-b99"
    result = validate_event(encode_event(event), now=NOW)
    assert result.reason == "bad_sensor_id"


def test_bad_timestamp_is_rejected():
    """Sin event_time legible no hay tiempo de evento."""
    event = valid_raw_event()
    event["event_time"] = "not-a-timestamp"
    result = validate_event(encode_event(event), now=NOW)
    assert result.reason == "bad_timestamp"


def test_non_string_event_time_is_bad_timestamp_instead_of_raising_later():
    """Un event_time numérico pasaría str() y tumbaría el job al asignar el timestamp."""
    for value in (20260925, 1790351105.5, None, ["2026-09-25T10:00:00Z"]):
        event = valid_raw_event()
        event["event_time"] = value
        result = validate_event(encode_event(event), now=NOW)
        assert result.reason == "bad_timestamp", value


def test_stale_clock_is_rejected_when_far_in_the_past():
    """Un reloj de origen muy atrasado va a la DLQ."""
    event = valid_raw_event()
    event["event_time"] = "2000-01-01T00:00:00Z"
    result = validate_event(encode_event(event), now=NOW)
    assert result.reason == "stale_clock"


def test_event_time_within_replay_skew_is_accepted():
    """El replay acelerado adelanta event_time hasta ~2h51min (tramo 3h, speedup 20x)."""
    event = valid_raw_event()
    event["event_time"] = "2020-12-19T07:00:12Z"  # NOW + 2h30min
    result = validate_event(encode_event(event), now=NOW)
    assert result.is_valid


def test_event_time_far_beyond_replay_skew_is_rejected():
    """Más allá de la tolerancia del replay es stale_clock."""
    event = valid_raw_event()
    event["event_time"] = "2020-12-19T10:00:12Z"  # NOW + 5h30min
    result = validate_event(encode_event(event), now=NOW)
    assert result.reason == "stale_clock"


def test_temp_missing_for_real_empty_cell():
    """La celda vacía real del CSV (temp_c null) va a la DLQ."""
    event = valid_raw_event()
    event["payload"]["temp_c"] = None
    result = validate_event(encode_event(event), now=NOW)
    assert result.reason == "temp_missing"


def test_temp_not_numeric_for_injected_text_value():
    """El texto inyectado en temp_c va a la DLQ."""
    event = valid_raw_event()
    event["payload"]["temp_c"] = "n/a"
    result = validate_event(encode_event(event), now=NOW)
    assert result.reason == "temp_not_numeric"


def test_missing_temp_c_field_for_injected_removal():
    """El temp_c quitado por la inyección va a la DLQ."""
    event = valid_raw_event()
    del event["payload"]["temp_c"]
    result = validate_event(encode_event(event), now=NOW)
    assert result.reason == "missing_field:payload.temp_c"


def test_temp_out_of_physical_range_is_rejected():
    """Una temperatura físicamente imposible va a la DLQ."""
    event = valid_raw_event()
    event["payload"]["temp_c"] = -200.0
    result = validate_event(encode_event(event), now=NOW)
    assert result.reason == "temp_out_of_physical_range"


def test_canonical_json_round_trip_is_stable():
    """JSON canónico, mismos bytes para el mismo evento aunque cambie el orden."""
    event = valid_raw_event()
    reordered = {key: event[key] for key in reversed(list(event))}
    reordered["payload"] = {key: event["payload"][key] for key in reversed(list(event["payload"]))}
    assert list(reordered) != list(event)
    first = encode_event(event)
    second = encode_event(reordered)
    assert first == second
    assert b" " not in first
    assert validate_event(first, now=NOW).event == validate_event(second, now=NOW).event
