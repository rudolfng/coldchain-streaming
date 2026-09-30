"""Calendario, retención/liberación, duplicados y determinismo del replay.

Usa un productor falso (sin Kafka): las pruebas no dependen de Docker.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

import coldchain_streaming.producer as producer_module
from coldchain_streaming.producer import (
    BURST_RESEND,
    RETENTION_WINDOWS,
    ColdchainReplay,
    aligned_start_delay,
    build_raw_bytes,
    plan_replay,
)

BASE_SOURCE_TIME = datetime(2020, 12, 19, 0, 0, 0, tzinfo=UTC)


class FakeProducer:
    """Sustituto de `confluent_kafka.Producer` que solo acumula mensajes en memoria.

    `fail_every` > 0 hace que uno de cada N mensajes vuelva con error de entrega.
    """

    class _FakeMessage:
        def __init__(self, partition: int, topic: str) -> None:
            self._partition = partition
            self._topic = topic

        def partition(self) -> int:
            return self._partition

        def topic(self) -> str:
            return self._topic

    def __init__(self, fail_every: int = 0) -> None:
        self.messages: list[dict[str, object]] = []
        self.fail_every = fail_every

    def produce(self, topic, key=None, value=None, timestamp=None, on_delivery=None):
        self.messages.append({"topic": topic, "key": key, "value": value, "timestamp": timestamp})
        if on_delivery is not None:
            failed = self.fail_every and len(self.messages) % self.fail_every == 0
            on_delivery("broker caido" if failed else None, self._FakeMessage(0, topic))

    def poll(self, timeout: float = 0):
        return 0

    def flush(self, timeout: float = 0):
        return 0


def _row(sensor_id: str, elapsed_s: float, *, temp_f: float = -113.0, temp_c: float = -80.56):
    return {
        "sensor_id": sensor_id,
        "elapsed_s": elapsed_s,
        "source_time": pd.Timestamp(BASE_SOURCE_TIME + timedelta(seconds=elapsed_s)),
        "temp_f": temp_f,
        "temp_c": temp_c,
    }


def demo_scenario_frame() -> pd.DataFrame:
    """Sensores y tiempos alrededor de las ventanas de la inyección `demo`."""
    b07_start, b07_end, _ = RETENTION_WINDOWS["demo"]["ULT1-b07"]
    rows = []
    # b07: dos lecturas antes, tres dentro de la ventana retenida ([0, 360)), dos después
    # (400/460 evitan coincidir con el instante de liberación, 360).
    for offset in (-120, -60, 60, 180, 300, 400, 460):
        rows.append(_row("ULT1-b07", b07_start + offset))
    # sensor de control: nunca se retiene.
    for offset in range(-120, 400, 60):
        rows.append(_row("ULT1-b01", b07_start + offset))
    # b03: 25 lecturas cada 30s antes de la ráfaga (66:50), para probar que se toman las últimas 20.
    burst_sensor, burst_at_s, burst_count = BURST_RESEND["demo"]
    for index in range(25):
        rows.append(_row(burst_sensor, burst_at_s - (25 - index) * 30))
    # y una lectura de b03 posterior a la ráfaga, que no debe reenviarse.
    rows.append(_row(burst_sensor, burst_at_s + 30))
    frame = pd.DataFrame(rows)
    return frame.sort_values(["elapsed_s", "sensor_id"], kind="stable").reset_index(drop=True)


def test_retention_delays_emission_but_not_event_time():
    """La retención atrasa la emisión, no el tiempo de evento."""
    frame = demo_scenario_frame()
    items = plan_replay(frame, seed=7, injections="demo", duplicate_rate=0, invalid_rate=0)
    retained = [
        item
        for item in items
        if item.kind == "normal"
        and item.sensor_id == "ULT1-b07"
        and item.emit_offset_s != item.offset_s
    ]
    assert len(retained) == 3
    window_start, window_end, release = RETENTION_WINDOWS["demo"]["ULT1-b07"]
    elapsed_min = float(frame["elapsed_s"].min())
    for item in retained:
        # el evento se emite en la liberación...
        assert item.emit_offset_s == release - elapsed_min
        # ...pero su event_time sigue anclado al instante real de la lectura.
        t0 = datetime(2026, 1, 1, tzinfo=UTC)
        _, _, event_time = build_raw_bytes(item, t0_event=t0)
        assert event_time == t0 + timedelta(seconds=item.offset_s)
        assert item.offset_s != item.emit_offset_s


def test_retained_events_are_emitted_in_a_burst_after_control_sensor():
    """Lo retenido de b07 sale en ráfaga, después del sensor de control."""
    frame = demo_scenario_frame()
    items = plan_replay(frame, seed=7, injections="demo", duplicate_rate=0, invalid_rate=0)
    position = {(item.sensor_id, item.offset_s): index for index, item in enumerate(items)}
    retained = [
        item
        for item in items
        if item.sensor_id == "ULT1-b07" and item.emit_offset_s != item.offset_s
    ]
    assert len(retained) == 3
    # las retenidas salen juntas y seguidas, en el instante de liberación...
    assert len({item.emit_offset_s for item in retained}) == 1
    positions = sorted(position[("ULT1-b07", item.offset_s)] for item in retained)
    assert positions == list(range(positions[0], positions[0] + 3))
    # ...después de la lectura del sensor de control tomada en el mismo instante real, que sale
    # a tiempo.
    for item in retained:
        control = position[("ULT1-b01", item.offset_s)]
        assert control < position[("ULT1-b07", item.offset_s)]
        assert items[control].emit_offset_s == items[control].offset_s


def test_burst_resend_takes_the_last_n_events_of_the_target_sensor():
    """La ráfaga reenvía las últimas N lecturas del sensor."""
    frame = demo_scenario_frame()
    items = plan_replay(frame, seed=7, injections="demo", duplicate_rate=0, invalid_rate=0)
    burst_sensor, burst_at_s, burst_count = BURST_RESEND["demo"]
    burst_items = [item for item in items if item.kind == "burst_resend"]
    assert len(burst_items) == burst_count
    elapsed_min = float(frame["elapsed_s"].min())
    assert all(item.emit_offset_s == burst_at_s - elapsed_min for item in burst_items)
    # deben ser las últimas `burst_count` lecturas de b03 anteriores a la ráfaga, en orden original:
    # ni las primeras 5 de las 25 previas ni la posterior a la ráfaga.
    expected = [
        burst_at_s - (25 - index) * 30 - elapsed_min for index in range(25 - burst_count, 25)
    ]
    assert [item.offset_s for item in burst_items] == expected


def test_burst_resend_has_identical_bytes_to_the_original():
    """Un reenvío es byte a byte el original (mismo event_id)."""
    frame = demo_scenario_frame()
    items = plan_replay(frame, seed=7, injections="demo", duplicate_rate=0, invalid_rate=0)
    burst_items = [item for item in items if item.kind == "burst_resend"]
    normal_items = {
        (item.sensor_id, item.offset_s): item for item in items if item.kind == "normal"
    }
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    for burst_item in burst_items:
        original = normal_items[(burst_item.sensor_id, burst_item.offset_s)]
        original_bytes, original_id, _ = build_raw_bytes(original, t0_event=t0)
        burst_bytes, burst_id, _ = build_raw_bytes(burst_item, t0_event=t0)
        assert original_bytes == burst_bytes
        assert original_id == burst_id


def test_duplicates_share_event_id_and_bytes_with_the_original():
    """Un duplicado inyectado comparte event_id y bytes con el original."""
    frame = demo_scenario_frame()
    items = plan_replay(frame, seed=7, injections="none", duplicate_rate=1.0, invalid_rate=0)
    normal = [item for item in items if item.kind == "normal"]
    duplicates = [item for item in items if item.kind == "duplicate"]
    assert len(duplicates) == len(normal)  # tasa 1.0: todos se duplican
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    by_key = {}
    for item in items:
        raw, event_id, _ = build_raw_bytes(item, t0_event=t0)
        by_key.setdefault((item.sensor_id, item.offset_s), []).append((event_id, raw))
    for pairs in by_key.values():
        ids = {event_id for event_id, _ in pairs}
        bytes_values = {raw for _, raw in pairs}
        assert len(ids) == 1
        assert len(bytes_values) == 1


def test_invalid_injection_keeps_event_id_intact():
    """El inválido inyectado conserva su event_id para rastrearlo en la DLQ."""
    frame = demo_scenario_frame()
    items = plan_replay(frame, seed=7, injections="none", duplicate_rate=0, invalid_rate=1.0)
    assert any(item.invalid_mode is not None for item in items)
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    for item in items:
        raw, event_id, _ = build_raw_bytes(item, t0_event=t0)
        decoded = json.loads(raw)
        assert decoded["event_id"] == event_id
        assert decoded["event_id"]  # nunca vacío
        if item.invalid_mode == "remove_temp_c":
            assert "temp_c" not in decoded["payload"]
        elif item.invalid_mode == "temp_c_text":
            assert decoded["payload"]["temp_c"] == "n/a"


def test_same_seed_produces_the_same_sequence():
    """Misma semilla, misma corrida."""
    frame = demo_scenario_frame()
    first = plan_replay(frame, seed=7, injections="demo", duplicate_rate=0.3, invalid_rate=0.2)
    second = plan_replay(frame, seed=7, injections="demo", duplicate_rate=0.3, invalid_rate=0.2)
    as_tuples = lambda items: [  # noqa: E731
        (item.kind, item.sensor_id, item.offset_s, item.emit_offset_s, item.invalid_mode)
        for item in items
    ]
    assert as_tuples(first) == as_tuples(second)


def test_different_seed_can_change_the_sequence():
    """La semilla controla las inyecciones."""
    frame = demo_scenario_frame()
    a = plan_replay(frame, seed=1, injections="demo", duplicate_rate=0.5, invalid_rate=0.5)
    b = plan_replay(frame, seed=2, injections="demo", duplicate_rate=0.5, invalid_rate=0.5)
    assert a != b


class FakeClock:
    """Reemplaza el módulo `time` del productor: `sleep` avanza el reloj sin esperar."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def test_replayed_event_times_start_at_the_start_instant():
    """El tramo arranca en el instante de arranque y conserva la separación real."""
    frame = demo_scenario_frame()
    items = plan_replay(frame, seed=7, injections="demo", duplicate_rate=0, invalid_rate=0)
    fake = FakeProducer()
    before = datetime.now(UTC)
    ColdchainReplay(fake, topic="t", speedup=1_000_000).replay(items, realtime=False)
    after = datetime.now(UTC)
    event_times = [datetime.fromtimestamp(m["timestamp"] / 1000, UTC) for m in fake.messages]
    # la primera lectura del tramo cae en el arranque y el resto conserva la separación real.
    assert before - timedelta(milliseconds=1) <= min(event_times) <= after
    span = max(item.offset_s for item in items) - min(item.offset_s for item in items)
    assert (max(event_times) - min(event_times)).total_seconds() == pytest.approx(span, abs=0.01)


def test_calendar_preserves_event_time_gaps_and_speedup_only_compresses_the_wait(monkeypatch):
    """Speedup comprime la espera, no el tiempo de evento."""
    frame = demo_scenario_frame()
    items = plan_replay(frame, seed=7, injections="none", duplicate_rate=0, invalid_rate=0)
    clock = FakeClock()
    monkeypatch.setattr(producer_module, "time", clock)
    fake = FakeProducer()
    speedup = 20
    ColdchainReplay(fake, topic="coldchain.telemetry.raw.v1", speedup=speedup).replay(
        items, realtime=True
    )
    # el reloj de pared recorre el tramo dividido por speedup...
    last_emit = max(item.emit_offset_s for item in items)
    assert clock.now == pytest.approx(last_emit / speedup, abs=0.1)
    assert len(fake.messages) == len(items)
    timestamps = [message["timestamp"] for message in fake.messages]
    # ...pero el event_time (timestamp de Kafka) conserva la separación real, sin comprimir.
    offsets = sorted(item.offset_s for item in items)
    expected_span_ms = (offsets[-1] - offsets[0]) * 1000
    observed_span_ms = max(timestamps) - min(timestamps)
    assert observed_span_ms == pytest.approx(expected_span_ms, abs=1)


def test_failed_delivery_is_counted_and_not_reported_as_delivered():
    """El resumen distingue lo intentado de lo que Kafka confirmó."""
    frame = demo_scenario_frame()
    items = plan_replay(frame, seed=7, injections="none", duplicate_rate=0, invalid_rate=0)
    fake = FakeProducer(fail_every=4)
    result = ColdchainReplay(fake, topic="t", speedup=1_000_000).replay(items, realtime=False)
    failed = len(items) // 4
    assert result["events"] == len(items)
    assert result["delivery_errors"] == failed
    assert result["delivered"] == len(items) - failed


def test_aligned_start_puts_the_slice_start_on_a_window_edge():
    """Con la alineación, cada inyección cae en ventanas completas (demo 1x)."""
    # Caso real del subtramo 66:15: la primera muestra es 66:15:06 (elapsed 238 506 s).
    first_elapsed = 66 * 3600 + 15 * 60 + 6
    delay = aligned_start_delay(now_epoch=1000.0, align_seconds=300, lead_seconds=first_elapsed)
    start = 1000.0 + delay
    assert delay == pytest.approx(206.0)
    # El minuto 66:40:00 del ensayo (inicio de la retención de b12) cae justo en un borde:
    # su event_time es start + (66:40:00 - primera muestra).
    assert (start + (66 * 3600 + 40 * 60 - first_elapsed)) % 300 == 0
    # Solo importa el resto módulo la ventana; si ya se está en el borde, no se espera, y nunca
    # se espera una ventana entera.
    assert aligned_start_delay(1000.0, 300, 6) == delay
    assert aligned_start_delay(1206.0, 300, first_elapsed) == 0
    assert 0 <= aligned_start_delay(1207.0, 300, first_elapsed) < 300
