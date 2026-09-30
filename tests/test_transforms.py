"""CombineFn incremental, dedup y formato de panes con TestPipeline."""

from __future__ import annotations

from datetime import UTC, datetime

import apache_beam as beam
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

import coldchain_streaming.transforms as transforms_module
from coldchain_streaming.config import Settings
from coldchain_streaming.transforms import (
    ParseAndValidate,
    TempStatsCombineFn,
    assign_event_timestamp,
    build_metrics,
    is_non_empty_pane,
)


def _reading(event_id: str, temp_c: float) -> dict:
    return {"event_id": event_id, "payload": {"temp_c": temp_c}}


def test_temp_stats_combine_fn_computes_n_avg_min_max():
    """Combinador incremental con n, promedio, mínimo y máximo."""
    combine_fn = TempStatsCombineFn()
    accumulator = combine_fn.create_accumulator()
    for index, temp_c in enumerate((-80.0, -79.0, -81.0)):
        accumulator = combine_fn.add_input(accumulator, _reading(f"e{index}", temp_c))
    assert combine_fn.extract_output(accumulator) == {
        "n_readings": 3,
        "avg_temp_c": -80.0,
        "min_temp_c": -81.0,
        "max_temp_c": -79.0,
        "out_of_range": True,  # min -81 < -80
    }


def test_repeated_event_id_does_not_grow_the_count():
    """La dedup por event_id vive en el acumulador."""
    combine_fn = TempStatsCombineFn()
    accumulator = combine_fn.create_accumulator()
    accumulator = combine_fn.add_input(accumulator, _reading("e1", -80.0))
    accumulator = combine_fn.add_input(accumulator, _reading("e1", -80.0))  # duplicado exacto
    accumulator = combine_fn.add_input(accumulator, _reading("e2", -79.0))
    output = combine_fn.extract_output(accumulator)
    assert output["n_readings"] == 2
    assert output["avg_temp_c"] == -79.5


def _single_reading_output(temp_c: float) -> dict:
    combine_fn = TempStatsCombineFn()
    accumulator = combine_fn.add_input(combine_fn.create_accumulator(), _reading("e1", temp_c))
    return combine_fn.extract_output(accumulator)


def test_out_of_range_flags_the_warm_edge():
    """Por encima de -60 C es fuera de rango."""
    assert _single_reading_output(-59.0)["out_of_range"] is True  # max > -60


def test_out_of_range_flags_the_cold_edge():
    """Por debajo de -80 C es fuera de rango."""
    assert _single_reading_output(-90.0)["out_of_range"] is True  # min < -80


def test_in_range_reading_is_not_flagged():
    """Dentro de -80/-60 C no se marca."""
    assert _single_reading_output(-70.0)["out_of_range"] is False


def test_pane_from_an_empty_accumulator_is_dropped():
    """Un pane vacío no llega al sink, donde pisaría datos."""
    combine_fn = TempStatsCombineFn()
    empty = combine_fn.extract_output(combine_fn.create_accumulator())
    assert is_non_empty_pane(empty) is False
    assert is_non_empty_pane(_single_reading_output(-80.5)) is True


def test_merge_accumulators_combines_partial_results():
    """El merge de acumuladores parciales es correcto."""
    combine_fn = TempStatsCombineFn()
    left = combine_fn.add_input(combine_fn.create_accumulator(), _reading("e1", -80.0))
    right = combine_fn.add_input(combine_fn.create_accumulator(), _reading("e2", -82.0))
    merged = combine_fn.merge_accumulators([left, right])
    output = combine_fn.extract_output(merged)
    assert output["n_readings"] == 2
    assert output["min_temp_c"] == -82.0
    assert output["max_temp_c"] == -80.0


def test_merge_accumulators_deduplicates_shared_event_id():
    """Dos acumuladores parciales (p.ej. de dos shards) pueden compartir un event_id
    si el mismo evento fue procesado en ambos; el merge no debe duplicarlo."""
    combine_fn = TempStatsCombineFn()
    left = combine_fn.add_input(combine_fn.create_accumulator(), _reading("e1", -80.0))
    right = combine_fn.add_input(combine_fn.create_accumulator(), _reading("e1", -80.0))
    merged = combine_fn.merge_accumulators([left, right])
    assert combine_fn.extract_output(merged)["n_readings"] == 1


def make_event(
    event_id: str,
    *,
    sensor_id: str = "ULT1-b07",
    event_time: str = "2026-09-25T14:20:05Z",
    temp_c: float = -80.5,
) -> dict:
    return {
        "schema_version": 2,
        "event_id": event_id,
        "event_time": event_time,
        "sensor_id": sensor_id,
        "asset_id": "ULT1",
        "asset_type": "contenedor",
        "payload": {"temp_c": temp_c, "temp_f": None, "source_time": event_time},
    }


def test_deduplication_collapses_three_copies_into_one_reading():
    """Tres copias del mismo evento cuentan una vez en el pipeline."""
    settings = Settings(window_seconds=300, allowed_lateness_seconds=0)
    events = [make_event("e1"), make_event("e1"), make_event("e1")]
    with TestPipeline() as pipeline:
        metrics = build_metrics(pipeline | beam.Create(events), settings, streaming_triggers=False)
        counts = metrics | beam.Map(lambda metric: metric["n_readings"])
        assert_that(counts, equal_to([1]))


def test_format_pane_has_stable_aggregate_id_and_window_bounds():
    """Clave estable y bordes [inicio, fin) de la ventana."""
    settings = Settings(window_seconds=300, allowed_lateness_seconds=0)
    events = [make_event("e1", sensor_id="ULT1-b07", event_time="2026-09-25T14:20:05Z")]
    with TestPipeline() as pipeline:
        metrics = build_metrics(pipeline | beam.Create(events), settings, streaming_triggers=False)
        fields = metrics | beam.Map(
            lambda m: (m["aggregate_id"], m["sensor_id"], m["window_start"], m["window_end"])
        )
        assert_that(
            fields,
            equal_to(
                [
                    (
                        "ULT1-b07|2026-09-25T14:20:00Z",
                        "ULT1-b07",
                        "2026-09-25T14:20:00Z",
                        "2026-09-25T14:25:00Z",
                    )
                ]
            ),
        )


def test_windows_partition_by_sensor_and_five_minutes():
    """Ventanas por sensor; un evento en fin cae en la ventana siguiente."""
    settings = Settings(window_seconds=300, allowed_lateness_seconds=0)
    events = [
        make_event("e1", sensor_id="ULT1-b01", event_time="2026-09-25T14:20:05Z", temp_c=-80.0),
        make_event("e2", sensor_id="ULT1-b01", event_time="2026-09-25T14:24:55Z", temp_c=-81.0),
        make_event("e3", sensor_id="ULT1-b01", event_time="2026-09-25T14:25:01Z", temp_c=-79.5),
        make_event("e4", sensor_id="ULT1-b02", event_time="2026-09-25T14:20:05Z", temp_c=-70.0),
    ]
    with TestPipeline() as pipeline:
        metrics = build_metrics(pipeline | beam.Create(events), settings, streaming_triggers=False)
        readings = metrics | beam.Map(
            lambda m: (m["sensor_id"], m["window_start"], m["n_readings"])
        )
        assert_that(
            readings,
            equal_to(
                [
                    ("ULT1-b01", "2026-09-25T14:20:00Z", 2),
                    ("ULT1-b01", "2026-09-25T14:25:00Z", 1),
                    ("ULT1-b02", "2026-09-25T14:20:00Z", 1),
                ]
            ),
        )


def test_parse_and_validate_separates_invalid_output():
    """Lo inválido sale por la salida lateral."""
    now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    valid = (
        b"ULT1-b07",
        (
            '{"schema_version":2,"event_id":"x","event_time":'
            f'"{now}","sensor_id":"ULT1-b07","asset_id":"ULT1",'
            '"asset_type":"contenedor","payload":{"temp_c":-80.5}}'
        ).encode(),
    )
    invalid = (b"unknown", b"{not json")
    with TestPipeline() as pipeline:
        parsed = (
            pipeline
            | beam.Create([valid, invalid])
            | beam.ParDo(ParseAndValidate()).with_outputs(ParseAndValidate.INVALID, main="valid")
        )
        valid_ids = parsed.valid | beam.Map(lambda event: event["event_id"])
        invalid_reasons = parsed.invalid | beam.Map(lambda record: record["reason"])
        assert_that(valid_ids, equal_to(["x"]), label="valid")
        assert_that(invalid_reasons, equal_to(["invalid_json"]), label="invalid")


def test_parse_and_validate_counts_valid_and_invalid_by_reason():
    """Contadores de Beam por resultado; el motivo va sin su sufijo de campo."""
    now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    valid = (
        '{"schema_version":2,"event_id":"x","event_time":'
        f'"{now}","sensor_id":"ULT1-b07","asset_id":"ULT1",'
        '"asset_type":"contenedor","payload":{"temp_c":-80.5}}'
    ).encode()
    missing_field = b'{"schema_version":2}'
    pipeline = TestPipeline()
    _ = (
        pipeline
        | beam.Create([(b"k", valid), (b"k", b"{not json"), (b"k", b"[1]"), (b"k", missing_field)])
        | beam.ParDo(ParseAndValidate()).with_outputs(ParseAndValidate.INVALID, main="valid")
    )
    result = pipeline.run()
    result.wait_until_finish()
    counters = result.metrics().query(MetricsFilter().with_namespace("coldchain"))["counters"]
    totals = {counter.key.metric.name: counter.committed for counter in counters}
    assert totals == {"events_valid": 1, "invalid_invalid_json": 2, "invalid_missing_field": 1}


def test_parse_and_validate_sends_a_null_value_to_the_dlq():
    """Un valor nulo va a la DLQ sin tumbar el job."""
    (output,) = list(ParseAndValidate().process((b"ULT1-b07", None)))
    assert output.tag == ParseAndValidate.INVALID
    assert output.value["reason"] == "invalid_json"


def test_parse_and_validate_sends_an_unexpected_validator_error_to_the_dlq(monkeypatch):
    """Si el validador lanza algo no previsto, el mensaje va a la DLQ y el job sigue."""

    def broken_validator(raw):
        raise RuntimeError("caso no previsto")

    monkeypatch.setattr(transforms_module, "validate_event", broken_validator)
    (output,) = list(ParseAndValidate().process((b"ULT1-b07", b'{"event_id":"e9"}')))
    assert output.tag == ParseAndValidate.INVALID
    assert output.value["reason"] == "unexpected:RuntimeError"
    assert output.value["event_id"] == "e9"


def test_parse_and_validate_survives_deeply_nested_json():
    """El rescate del event_id corre fuera de la red y tampoco puede lanzar."""
    raw = b"[" * 5000 + b"]" * 5000
    (output,) = list(ParseAndValidate().process((b"ULT1-b07", raw)))
    assert output.tag == ParseAndValidate.INVALID
    assert output.value["reason"] == "invalid_json"
    assert output.value["event_id"] is None


def test_event_time_without_timezone_is_read_as_utc():
    """Sin zona horaria el tiempo de evento es UTC, como en el validador."""
    naive = assign_event_timestamp(make_event("e1", event_time="2026-09-25T14:20:05"))
    aware = assign_event_timestamp(make_event("e1", event_time="2026-09-25T14:20:05Z"))
    assert naive.timestamp == aware.timestamp


def test_parse_and_validate_survives_an_integer_beyond_the_digit_limit():
    """El rescate del event_id tampoco lanza con un entero de más de 4300 dígitos."""
    raw = b'{"event_id":"e1","n":' + b"9" * 5000 + b"}"
    (output,) = list(ParseAndValidate().process((b"ULT1-b07", raw)))
    assert output.tag == ParseAndValidate.INVALID
    assert output.value["reason"] == "invalid_json"
