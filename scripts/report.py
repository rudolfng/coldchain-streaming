"""Vista de consola sobre la salida del pipeline: ventanas, tardíos, descartes y DLQ.

Lee `data/out/metrics.sqlite` (mismo sink que `consumer.py`) y, si hay un broker
disponible, cuenta la DLQ por motivo desde `earliest`. Su salida se guarda como evidencia
en `docs/evidencia/` antes y después de la ráfaga tardía (README secciones 4.1 y 9).
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import UTC, datetime

from confluent_kafka import Consumer, TopicPartition

from coldchain_streaming.config import Settings, sink_path
from coldchain_streaming.consumer import open_store


def _temp(value: float | None) -> str:
    return "      -" if value is None else f"{value:>7.2f}"


def print_windows(sink) -> None:
    rows = sink.execute(
        """
        SELECT sensor_id, window_start, n_readings, min_temp_c, max_temp_c, out_of_range,
               pane_timing, pane_index, is_final
        FROM metrics_5min
        ORDER BY sensor_id, window_start
        """
    ).fetchall()
    print(f"\n== Ventanas materializadas ({len(rows)}) ==")
    for row in rows:
        sensor_id, start, n, low, high, oor, timing, index, is_final = row
        flag = "FUERA DE RANGO" if oor else "ok"
        final = "final" if is_final else "abierta"
        print(
            f"{sensor_id}  {start}  n={n:>3}  min={_temp(low)}  max={_temp(high)}  "
            f"{flag:<15} ultimo_pane={timing}#{index} ({final})"
        )


def print_late_panes(sink) -> None:
    rows = sink.execute(
        "SELECT aggregate_id, pane_index, received_at FROM pane_log "
        "WHERE pane_timing = 'LATE' AND applied = 1 ORDER BY received_at"
    ).fetchall()
    print(f"\n== Paneles LATE aplicados ({len(rows)}) ==")
    for aggregate_id, pane_index, received_at in rows:
        print(f"{aggregate_id}  pane_index={pane_index}  recibido={received_at}")


def print_ignored_panes(sink) -> None:
    rows = sink.execute(
        "SELECT aggregate_id, pane_index, received_at FROM pane_log "
        "WHERE applied = 0 ORDER BY received_at"
    ).fetchall()
    print(f"\n== Paneles ignorados por ser mas viejos que el guardado ({len(rows)}) ==")
    for aggregate_id, pane_index, received_at in rows:
        print(f"{aggregate_id}  pane_index={pane_index}  recibido={received_at}")


def print_history(sink, sensor_id: str) -> None:
    """Panes de las ventanas de un sensor que recibieron más de un pane: la corrección de una
    ventana ya publicada (n y hora de llegada de cada pane)."""
    rows = sink.execute(
        """
        SELECT aggregate_id, pane_index, pane_timing, n_readings, received_at, applied
        FROM pane_log
        WHERE aggregate_id IN (
            SELECT aggregate_id FROM pane_log WHERE aggregate_id LIKE ? || '|%'
            GROUP BY aggregate_id HAVING COUNT(*) > 1
        )
        ORDER BY aggregate_id, received_at
        """,
        (sensor_id,),
    ).fetchall()
    print(f"\n== Historial de panes de {sensor_id} ({len(rows)} en ventanas con mas de uno) ==")
    for aggregate_id, pane_index, timing, n, received_at, applied in rows:
        state = "aplicado" if applied else "ignorado"
        print(f"{aggregate_id}  {timing}#{pane_index}  n={n:>3}  recibido={received_at}  {state}")


def count_dlq(settings: Settings, *, timeout_seconds: float = 10.0) -> Counter:
    """Cuenta la DLQ por motivo leyendo cada partición hasta su high watermark actual.

    Se fija el final antes de leer: el conteo es exacto para ese instante y no depende de que
    un `poll()` vuelva vacío a tiempo.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"coldchain-report-dlq-{id(settings)}",
            "enable.auto.commit": False,
        }
    )
    counts: Counter = Counter()
    try:
        metadata = consumer.list_topics(settings.topic_dlq, timeout=timeout_seconds)
        last_offset: dict[int, int] = {}
        assignment = []
        for partition in metadata.topics[settings.topic_dlq].partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(settings.topic_dlq, partition), timeout=timeout_seconds
            )
            if high > low:
                last_offset[partition] = high - 1
                assignment.append(TopicPartition(settings.topic_dlq, partition, low))
        consumer.assign(assignment)
        while last_offset:
            message = consumer.poll(timeout=timeout_seconds)
            if message is None:
                raise TimeoutError(f"particiones sin terminar de leer: {sorted(last_offset)}")
            if message.error():
                continue
            try:
                record = json.loads(message.value().decode())
                counts[record.get("reason", "unknown")] += 1
            except (json.JSONDecodeError, UnicodeDecodeError):
                counts["unparseable"] += 1
            if message.offset() >= last_offset.get(message.partition(), -1):
                last_offset.pop(message.partition(), None)
    finally:
        consumer.close()
    return counts


def print_dlq(settings: Settings) -> None:
    try:
        counts = count_dlq(settings)
    except Exception as error:  # noqa: BLE001 - reporte best-effort, no debe romper el resto
        print(f"\n== DLQ ({settings.topic_dlq}): no disponible ({error}) ==")
        return
    print(f"\n== DLQ por motivo ({sum(counts.values())} eventos) ==")
    for reason, count in counts.most_common():
        print(f"{reason:<32} {count}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--skip-dlq", action="store_true", help="no consultar el topico de DLQ")
    group.add_argument("--dlq-only", action="store_true", help="solo el conteo de la DLQ")
    group.add_argument("--history", metavar="SENSOR_ID", help="historial de panes de un sensor")
    args = parser.parse_args()

    settings = Settings.from_env()
    generated = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    print(
        f"Reporte generado {generated} (sink {sink_path(settings)}, broker "
        f"{settings.kafka_bootstrap_servers})"
    )
    if args.history:
        sink = open_store(sink_path(settings))
        print_history(sink, args.history)
        sink.close()
        return
    if not args.dlq_only:
        sink = open_store(sink_path(settings))
        print_windows(sink)
        print_late_panes(sink)
        print_ignored_panes(sink)
        sink.close()
    if not args.skip_dlq:
        print_dlq(settings)


if __name__ == "__main__":
    main()
