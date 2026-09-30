"""Upsert idempotente en SQLite y sondeo del sink: pane mayor gana, offsets al final.

SQLite en `tmp_path` y un consumidor falso, sin Kafka ni Docker.
"""

from __future__ import annotations

import json

from confluent_kafka import KafkaError

from coldchain_streaming.consumer import open_store, poll_into_store, upsert_metric


def metric(
    *,
    sensor_id="ULT1-b07",
    window_start="2026-09-25T14:20:00Z",
    pane_index=0,
    pane_timing="ON_TIME",
    n_readings=1,
    max_temp_c=-80.0,
    is_final=False,
):
    return {
        "aggregate_id": f"{sensor_id}|{window_start}",
        "sensor_id": sensor_id,
        "window_start": window_start,
        "window_end": "2026-09-25T14:25:00Z",
        "n_readings": n_readings,
        "avg_temp_c": -80.0,
        "min_temp_c": -81.0,
        "max_temp_c": max_temp_c,
        "out_of_range": True,
        "pane_timing": pane_timing,
        "pane_index": pane_index,
        "is_final": is_final,
        "emitted_at": "2026-09-25T14:25:05Z",
    }


def test_first_pane_is_applied(tmp_path):
    """El primer pane de una ventana crea la fila."""
    connection = open_store(tmp_path / "metrics.sqlite")
    applied = upsert_metric(connection, metric(pane_index=0, n_readings=1))
    assert applied is True
    row = connection.execute(
        "SELECT n_readings, pane_index FROM metrics_5min WHERE sensor_id = 'ULT1-b07'"
    ).fetchone()
    assert row == (1, 0)


def test_higher_pane_index_overwrites_the_row(tmp_path):
    """Un pane posterior corrige la ventana."""
    connection = open_store(tmp_path / "metrics.sqlite")
    upsert_metric(connection, metric(pane_index=0, n_readings=1, max_temp_c=-79.0))
    applied = upsert_metric(
        connection, metric(pane_index=1, pane_timing="LATE", n_readings=2, max_temp_c=-78.0)
    )
    assert applied is True
    row = connection.execute(
        "SELECT n_readings, max_temp_c, pane_index FROM metrics_5min WHERE sensor_id = 'ULT1-b07'"
    ).fetchone()
    assert row == (2, -78.0, 1)


def test_older_pane_index_is_ignored_and_logged(tmp_path):
    """Un pane viejo reenviado no pisa uno nuevo."""
    connection = open_store(tmp_path / "metrics.sqlite")
    upsert_metric(connection, metric(pane_index=2, n_readings=3))
    applied = upsert_metric(connection, metric(pane_index=1, n_readings=2))
    assert applied is False
    row = connection.execute(
        "SELECT n_readings, pane_index FROM metrics_5min WHERE sensor_id = 'ULT1-b07'"
    ).fetchone()
    assert row == (3, 2)  # el pane más nuevo se conserva, el viejo no lo pisa
    ignored = connection.execute("SELECT applied FROM pane_log WHERE pane_index = 1").fetchone()
    assert ignored == (0,)


def test_exact_repetition_is_harmless(tmp_path):
    """Reaplicar el mismo pane es idempotente."""
    connection = open_store(tmp_path / "metrics.sqlite")
    upsert_metric(connection, metric(pane_index=1, n_readings=2))
    applied_again = upsert_metric(connection, metric(pane_index=1, n_readings=2))
    assert applied_again is True  # pane_index igual: se re-aplica el mismo pane, sin efecto
    row = connection.execute(
        "SELECT n_readings FROM metrics_5min WHERE sensor_id = 'ULT1-b07'"
    ).fetchone()
    assert row == (2,)


def test_is_final_is_persisted(tmp_path):
    """Is_final llega a la vista materializada."""
    connection = open_store(tmp_path / "metrics.sqlite")
    upsert_metric(connection, metric(pane_index=3, is_final=True))
    row = connection.execute(
        "SELECT is_final FROM metrics_5min WHERE sensor_id = 'ULT1-b07'"
    ).fetchone()
    assert row == (1,)


def test_every_pane_is_logged_regardless_of_being_applied(tmp_path):
    """Pane_log registra todo pane, aplicado o no."""
    connection = open_store(tmp_path / "metrics.sqlite")
    upsert_metric(connection, metric(pane_index=2))
    upsert_metric(connection, metric(pane_index=0))  # ignorado
    upsert_metric(connection, metric(pane_index=3))  # aplicado
    count = connection.execute("SELECT COUNT(*) FROM pane_log").fetchone()[0]
    assert count == 3


def test_pane_log_keeps_the_reading_count_of_every_pane(tmp_path):
    """La corrección de una ventana queda visible pane a pane."""
    connection = open_store(tmp_path / "metrics.sqlite")
    upsert_metric(connection, metric(pane_index=0, pane_timing="EARLY", n_readings=1))
    upsert_metric(connection, metric(pane_index=1, pane_timing="EARLY", n_readings=30))
    rows = connection.execute(
        "SELECT pane_index, n_readings FROM pane_log ORDER BY pane_index"
    ).fetchall()
    assert rows == [(0, 1), (1, 30)]  # la corrección de la ventana queda visible pane a pane


def test_different_sensors_do_not_collide(tmp_path):
    """La clave del upsert separa sensores."""
    connection = open_store(tmp_path / "metrics.sqlite")
    upsert_metric(connection, metric(sensor_id="ULT1-b01", pane_index=0))
    upsert_metric(connection, metric(sensor_id="ULT1-b02", pane_index=0))
    count = connection.execute("SELECT COUNT(*) FROM metrics_5min").fetchone()[0]
    assert count == 2


def test_different_windows_of_the_same_sensor_do_not_collide(tmp_path):
    """La clave del upsert separa ventanas."""
    connection = open_store(tmp_path / "metrics.sqlite")
    upsert_metric(connection, metric(window_start="2026-09-25T14:20:00Z", pane_index=0))
    upsert_metric(connection, metric(window_start="2026-09-25T14:25:00Z", pane_index=0))
    count = connection.execute(
        "SELECT COUNT(*) FROM metrics_5min WHERE sensor_id = 'ULT1-b07'"
    ).fetchone()[0]
    assert count == 2


class FakeError:
    def __init__(self, code: int) -> None:
        self._code = code

    def code(self) -> int:
        return self._code


class FakeMessage:
    """Lo mínimo de `confluent_kafka.Message` que usa `poll_into_store`."""

    def __init__(self, value: bytes | None, *, offset: int = 0, error: FakeError | None = None):
        self._value, self._offset, self._error = value, offset, error

    def value(self) -> bytes | None:
        return self._value

    def error(self) -> FakeError | None:
        return self._error

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset


class FakeConsumer:
    """Devuelve un lote fijo y cuenta los commits de offsets."""

    def __init__(self, messages: list[FakeMessage]) -> None:
        self.messages = messages
        self.commits = 0

    def consume(self, num_messages: int, timeout: float) -> list[FakeMessage]:
        return self.messages

    def commit(self, asynchronous: bool) -> None:
        self.commits += 1


def _encoded(value: dict) -> bytes:
    return json.dumps(value).encode()


def test_poll_applies_the_batch_and_commits_offsets_once(tmp_path):
    """Los offsets se confirman después de escribir el lote en SQLite."""
    connection = open_store(tmp_path / "metrics.sqlite")
    consumer = FakeConsumer(
        [
            FakeMessage(_encoded(metric(sensor_id="ULT1-b01", n_readings=4)), offset=0),
            FakeMessage(_encoded(metric(sensor_id="ULT1-b02", n_readings=7)), offset=1),
        ]
    )
    result = poll_into_store(consumer, connection)
    assert result == {"applied": 2, "ignored": 0, "errors": 0, "max_n_readings": 7}
    assert consumer.commits == 1
    count = connection.execute("SELECT COUNT(*) FROM metrics_5min").fetchone()[0]
    assert count == 2


def test_poll_skips_damaged_panes_without_stopping_the_batch(tmp_path):
    """Un pane dañado no tumba el sink ni deja una escritura a medias."""
    connection = open_store(tmp_path / "metrics.sqlite")
    without_aggregate_id = metric(sensor_id="ULT1-b09")
    del without_aggregate_id["aggregate_id"]  # falla en pane_log, después de metrics_5min
    consumer = FakeConsumer(
        [
            FakeMessage(None, offset=0),  # tombstone
            FakeMessage(b"[1]", offset=1),  # JSON que no es objeto
            FakeMessage(_encoded({**metric(), "pane_index": None}), offset=2),
            FakeMessage(_encoded(without_aggregate_id), offset=3),
            FakeMessage(_encoded(metric(sensor_id="ULT1-b01")), offset=4),
        ]
    )
    result = poll_into_store(consumer, connection)
    assert result["errors"] == 4
    assert result["applied"] == 1
    assert consumer.commits == 1
    rows = connection.execute("SELECT sensor_id FROM metrics_5min").fetchall()
    assert rows == [("ULT1-b01",)]  # la fila de b09 se deshizo con el rollback


def test_poll_without_messages_does_not_commit(tmp_path):
    """El fin de partición no es un error ni confirma offsets; otro error se cuenta."""
    connection = open_store(tmp_path / "metrics.sqlite")
    eof = FakeMessage(None, error=FakeError(KafkaError._PARTITION_EOF))
    other = FakeMessage(None, error=FakeError(KafkaError._TRANSPORT))
    consumer = FakeConsumer([eof, other])
    result = poll_into_store(consumer, connection)
    assert result["errors"] == 1
    assert consumer.commits == 0
