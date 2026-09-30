"""Prueba de humo acotada: fuente -> Kafka -> Beam/Flink -> Kafka -> SQLite.

Corre con tópicos efímeros (se borran al terminar) y perfil `smoke` (tramo corto,
inyecciones mínimas), sin esperar al reloj real. Se ejecuta con `make smoke` (perfil Compose
`smoke`, requiere Docker).
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

from confluent_kafka.admin import AdminClient, NewTopic

from coldchain_streaming.config import Settings, processed_dataset_path
from coldchain_streaming.consumer import build_consumer, open_store, poll_into_store
from coldchain_streaming.producer import ColdchainReplay, build_producer, load_events, plan_replay

SMOKE_WINDOW_START_S = 65 * 3600
SMOKE_WINDOW_END_S = 65 * 3600 + 20 * 60  # tramo 65:00-65:20, ver README sección 8

logger = logging.getLogger("coldchain_streaming.smoke")


def max_readings_in_any_window(events, window_seconds: int) -> int:
    """Máximo de lecturas distintas de un sensor en cualquier intervalo [t, t + ventana).

    Es el techo de `n_readings` que puede tener una ventana si la dedup funciona, sin importar
    dónde caiga el borde de la ventana: 30 con muestreo exacto cada 10 s, 31 en el tramo real
    (el intervalo va de 9 a 11 s). Un duplicado o un reenvío contado de más lo supera.
    """
    best = 0
    for _, group in events.groupby("sensor_id"):
        times = sorted(group["elapsed_s"].unique())
        end = 0
        for start_index, start in enumerate(times):
            while end < len(times) and times[end] < start + window_seconds:
                end += 1
            best = max(best, end - start_index)
    return best


def fullest_window_without_dedup(items, window_seconds: int) -> int:
    """Ventana más llena si no hubiera dedup, en la PEOR alineación posible de las ventanas.

    Cuenta cada copia válida enviada (duplicados y reenvíos incluidos) por ventana fija, prueba
    todos los desfasajes de 1 s entre el inicio del tramo y el borde de ventana, y devuelve el
    mínimo de las ventanas más llenas. Si supera `max_readings_in_any_window`, la aserción de
    dedup detecta una dedup rota cualquiera sea el reloj en que arranque el smoke.
    """
    offsets: dict[str, list[float]] = {}
    for item in items:
        if item.invalid_mode is None:
            offsets.setdefault(item.sensor_id, []).append(item.offset_s)
    worst_case = None
    for phase in range(window_seconds):
        fullest = 0
        for times in offsets.values():
            counts: dict[int, int] = {}
            for offset in times:
                index = int((offset + phase) // window_seconds)
                counts[index] = counts.get(index, 0) + 1
            fullest = max(fullest, *counts.values())
        worst_case = fullest if worst_case is None else min(worst_case, fullest)
    return worst_case or 0


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    suffix = uuid.uuid4().hex[:8]
    topics = {
        "TOPIC_RAW": f"coldchain.smoke.raw.{suffix}",
        "TOPIC_METRICS": f"coldchain.smoke.metrics.{suffix}",
        "TOPIC_DLQ": f"coldchain.smoke.dlq.{suffix}",
    }
    os.environ.update(topics)
    settings = Settings.from_env()

    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    futures = admin.create_topics([NewTopic(topic, 4, 1) for topic in topics.values()])
    for future in futures.values():
        future.result(timeout=20)
    try:
        summary = run_smoke(settings, suffix)
    finally:
        # Sin esto cada `make smoke` dejaría tres tópicos más en el volumen kafka-data.
        for topic, future in admin.delete_topics(list(topics.values())).items():
            try:
                future.result(timeout=20)
            except Exception as error:  # noqa: BLE001 - la limpieza no tapa el resultado
                logger.warning("no se pudo borrar el topico %s: %s", topic, error)
    print(json.dumps(summary, indent=2, default=str))


def run_smoke(settings: Settings, suffix: str) -> dict[str, object]:
    """Replay acotado -> pipeline -> sink; devuelve el resumen o lanza si algo no cuadra."""
    events = load_events(processed_dataset_path())
    events = events[
        (events["elapsed_s"] >= SMOKE_WINDOW_START_S) & (events["elapsed_s"] < SMOKE_WINDOW_END_S)
    ]
    if events.empty:
        raise RuntimeError(
            "el parquet procesado no cubre el tramo 65:00-65:20; correr "
            "'python -m coldchain_streaming.dataset' antes del smoke"
        )

    items = plan_replay(
        events,
        seed=settings.replay_seed,
        injections="smoke",
        duplicate_rate=0.05,
        invalid_rate=0.02,
    )
    replay = ColdchainReplay(
        build_producer(settings.kafka_bootstrap_servers), topic=settings.topic_raw, speedup=10_000
    )
    produced = replay.replay(items, realtime=False)
    if produced["delivered"] != produced["events"]:
        raise RuntimeError(f"Kafka no confirmo todos los mensajes del replay: {produced}")
    total_sent = produced["delivered"]
    window_cap = max_readings_in_any_window(events, settings.window_seconds)
    window_without_dedup = fullest_window_without_dedup(items, settings.window_seconds)
    # Lecturas válidas distintas: los duplicados y reenvíos repiten un event_id ya contado y
    # los inválidos van a la DLQ.
    distinct_valid = sum(1 for item in items if item.kind == "normal" and item.invalid_mode is None)

    subprocess.run(
        [
            sys.executable,
            "-m",
            "coldchain_streaming.pipeline",
            "--group-id",
            f"smoke-beam-{suffix}",
            "--max-num-records",
            str(total_sent),
            "--max-read-time",
            "90",
        ],
        check=True,
        timeout=300,
        env=os.environ,
    )

    sink_file = Path(tempfile.gettempdir()) / f"coldchain-smoke-{suffix}.sqlite"
    connection = open_store(sink_file)
    consumer = build_consumer(settings, group_id=f"smoke-sink-{suffix}")
    max_n_readings = 0
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        poll_result = poll_into_store(consumer, connection, max_messages=500, timeout_seconds=1)
        max_n_readings = max(max_n_readings, poll_result["max_n_readings"])
    consumer.close()

    window_count = connection.execute("SELECT COUNT(*) FROM metrics_5min").fetchone()[0]
    total_readings = connection.execute(
        "SELECT COALESCE(SUM(n_readings), 0) FROM metrics_5min"
    ).fetchone()[0]
    late_panes = connection.execute(
        "SELECT COUNT(*) FROM pane_log WHERE pane_timing = 'LATE' AND applied = 1"
    ).fetchone()[0]
    connection.close()

    dlq_consumer = build_consumer(settings, group_id=f"smoke-dlq-{suffix}")
    dlq_consumer.subscribe([settings.topic_dlq])
    dlq_count = 0
    dlq_deadline = time.monotonic() + 20
    while time.monotonic() < dlq_deadline:
        messages = dlq_consumer.consume(num_messages=100, timeout=1)
        if not messages:
            continue
        dlq_count += sum(1 for m in messages if not m.error())
    dlq_consumer.close()

    summary = {
        "produced": produced,
        "windows": window_count,
        "total_readings": total_readings,
        "distinct_valid_sent": distinct_valid,
        "max_n_readings_observed": max_n_readings,
        "max_readings_per_window": window_cap,
        "max_readings_per_window_without_dedup": window_without_dedup,
        "late_panes_applied": late_panes,
        "dlq_count": dlq_count,
    }

    # No se exige un pane LATE acá: en un tramo acotado y sin tiempo real el watermark no
    # avanza de forma progresiva (README sección 4.1), así que el smoke no es el lugar para
    # demostrarlo. Eso lo prueba tests/test_windows.py con TestStream, de forma determinista.
    # Las dos igualdades de abajo son exactas a propósito: una cota (<=, >= 1) dejaba pasar un
    # pane vacío que pisaba ventanas o la pérdida del último lote de la DLQ.
    if window_without_dedup <= window_cap:
        raise RuntimeError(f"el escenario no distingue una dedup rota del tope real: {summary}")
    if window_count < 1:
        raise RuntimeError(f"no se materializo ninguna ventana: {summary}")
    if not 0 < max_n_readings <= window_cap:
        raise RuntimeError(
            f"una ventana conto mas lecturas que las distintas posibles (dedup rota): {summary}"
        )
    if total_readings != distinct_valid:
        raise RuntimeError(
            f"el sink no suma exactamente los eventos validos distintos enviados: {summary}"
        )
    # produced["invalid"] incluye los duplicados de inválidos: cada copia genera su registro.
    if dlq_count != produced["invalid"]:
        raise RuntimeError(f"la DLQ no recibio exactamente los invalidos enviados: {summary}")
    return summary


if __name__ == "__main__":
    main()
