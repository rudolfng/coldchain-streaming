"""Ventanas con TestStream: a tiempo, tardío aceptado, tardío descartado, early, duplicado.

Usa `TestStream` + `DirectRunner` en modo streaming: es la evidencia de
esta lógica, que la corrida real acelerada no muestra (README sección 4.1). Un único escenario
cubre los cinco casos:

(a) 3 lecturas en [10:00, 10:05) y watermark a 10:05:01 -> pane ON_TIME con n=3.
(b) una lectura de las 10:02 llega después -> pane LATE con n=4, max actualizado y
    pane_index + 1.
(c) el watermark pasa fin_de_ventana + 15 min y llega otra lectura de las 10:02 ->
    no se emite un pane nuevo (se descarta, más allá del lateness permitido).
(d) `advance_processing_time(6)` antes de que pase el watermark -> pane EARLY.
(e) un duplicado exacto (mismo `event_id`) antes del watermark -> `n_readings` no cambia; ni
    junto al original, ni como reintento después de que ya se publicó el pane EARLY.
"""

from __future__ import annotations

from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from coldchain_streaming.config import Settings
from coldchain_streaming.transforms import build_metrics


def _epoch(iso: str) -> float:
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()


def _event(event_id: str, event_time: str, *, temp_c: float) -> dict:
    return {
        "schema_version": 2,
        "event_id": event_id,
        "event_time": event_time,
        "sensor_id": "ULT1-b07",
        "asset_id": "ULT1",
        "asset_type": "contenedor",
        "payload": {"temp_c": temp_c, "temp_f": None, "source_time": event_time},
    }


def _timestamped(event_id: str, event_time: str, *, temp_c: float) -> TimestampedValue:
    return TimestampedValue(_event(event_id, event_time, temp_c=temp_c), _epoch(event_time))


WINDOW_START = "2026-09-25T10:00:00Z"
WINDOW_START_EPOCH = _epoch(WINDOW_START)


def build_test_stream() -> TestStream:
    return (
        TestStream()
        .advance_watermark_to(WINDOW_START_EPOCH)
        .add_elements(
            [
                _timestamped("e1", "2026-09-25T10:00:01Z", temp_c=-80.0),
                _timestamped("e1", "2026-09-25T10:00:01Z", temp_c=-80.0),  # (e) duplicado exacto
            ]
        )
        .advance_processing_time(6)  # (d) dispara el pane EARLY
        .add_elements(
            [
                _timestamped("e2", "2026-09-25T10:00:02Z", temp_c=-79.0),
                _timestamped("e3", "2026-09-25T10:02:00Z", temp_c=-81.0),
                # (e) reintento de e1 después de que ya se publicó un pane: tampoco cuenta
                _timestamped("e1", "2026-09-25T10:00:01Z", temp_c=-80.0),
            ]
        )
        .advance_watermark_to(WINDOW_START_EPOCH + 301)  # (a) pasa el fin de ventana
        .add_elements(
            [_timestamped("e4", "2026-09-25T10:02:15Z", temp_c=-78.0)]  # (b) tardío aceptado
        )
        .advance_watermark_to(WINDOW_START_EPOCH + 300 + 901)  # pasa window_end + 15 min
        .add_elements(
            [_timestamped("e5", "2026-09-25T10:02:20Z", temp_c=-60.0)]  # (c) demasiado tarde
        )
        .advance_watermark_to_infinity()
    )


def test_windows_early_on_time_late_accepted_late_dropped_and_duplicate():
    """Los cinco casos de panes con TestStream y watermark explícito."""
    settings = Settings(window_seconds=300, allowed_lateness_seconds=900, early_firing_seconds=5)
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with TestPipeline(options=options) as pipeline:
        events = pipeline | build_test_stream()
        metrics = build_metrics(events, settings, streaming_triggers=True)
        panes = metrics | beam.Map(
            lambda m: (m["pane_timing"], m["pane_index"], m["n_readings"], m["max_temp_c"])
        )
        expected = {
            # (d) EARLY: solo e1 estaba presente; (e) su duplicado no cuenta.
            "EARLY": ("EARLY", 0, 1, -80.0),
            # (a) ON_TIME: e1 + e2 + e3, el duplicado de e1 no infla el conteo.
            "ON_TIME": ("ON_TIME", 1, 3, -79.0),
            # (b) LATE: se suma e4 (n=4), max_temp_c pasa de -79.0 a -78.0, pane_index + 1.
            # (c) e5 llega pasado window_end + lateness: si no se descartara, habría un segundo
            # pane LATE (n=5) y esta lista de un solo elemento fallaría.
            "LATE": ("LATE", 2, 4, -78.0),
        }
        for timing, pane in expected.items():
            assert_that(
                panes | f"Solo {timing}" >> beam.Filter(lambda p, t=timing: p[0] == t),
                equal_to([pane]),
                label=f"pane {timing}",
            )
