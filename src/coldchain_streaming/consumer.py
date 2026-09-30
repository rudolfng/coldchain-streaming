"""Consumidor que materializa las métricas en SQLite de forma idempotente.

Upsert por `(sensor_id, window_start)` (regla en `upsert_metric`). Cada pane recibido queda
en `pane_log`, aplicado o ignorado (`applied = 0`).
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError

from coldchain_streaming.config import Settings, sink_path

logger = logging.getLogger("coldchain_streaming.consumer")

SCHEMA = """
CREATE TABLE IF NOT EXISTS metrics_5min (
  sensor_id TEXT NOT NULL,
  window_start TEXT NOT NULL,
  window_end TEXT NOT NULL,
  n_readings INTEGER,
  avg_temp_c REAL,
  min_temp_c REAL,
  max_temp_c REAL,
  out_of_range INTEGER,
  pane_timing TEXT,
  pane_index INTEGER,
  is_final INTEGER,
  emitted_at TEXT,
  updated_at TEXT,
  PRIMARY KEY (sensor_id, window_start)
);
CREATE TABLE IF NOT EXISTS pane_log (
  aggregate_id TEXT,
  pane_index INTEGER,
  pane_timing TEXT,
  n_readings INTEGER,
  received_at TEXT,
  applied INTEGER
);
"""


def open_store(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.executescript(SCHEMA)
    connection.commit()
    return connection


def upsert_metric(connection: sqlite3.Connection, metric: dict[str, Any]) -> bool:
    """Aplica un pane si no hay fila o si su `pane_index` es mayor o igual al guardado."""
    sensor_id = metric["sensor_id"]
    window_start = metric["window_start"]
    pane_index = int(metric["pane_index"])
    row = connection.execute(
        "SELECT pane_index FROM metrics_5min WHERE sensor_id = ? AND window_start = ?",
        (sensor_id, window_start),
    ).fetchone()
    applied = row is None or pane_index >= int(row[0])
    # Un job nuevo del pipeline reinicia pane_index en 0; sus panes se ignoran mientras la
    # fila guardada tenga un índice mayor (README sección 7).
    now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    if applied:
        connection.execute(
            """
            INSERT INTO metrics_5min (
                sensor_id, window_start, window_end, n_readings, avg_temp_c, min_temp_c,
                max_temp_c, out_of_range, pane_timing, pane_index, is_final, emitted_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (sensor_id, window_start) DO UPDATE SET
                window_end=excluded.window_end,
                n_readings=excluded.n_readings,
                avg_temp_c=excluded.avg_temp_c,
                min_temp_c=excluded.min_temp_c,
                max_temp_c=excluded.max_temp_c,
                out_of_range=excluded.out_of_range,
                pane_timing=excluded.pane_timing,
                pane_index=excluded.pane_index,
                is_final=excluded.is_final,
                emitted_at=excluded.emitted_at,
                updated_at=excluded.updated_at
            """,
            (
                sensor_id,
                window_start,
                metric["window_end"],
                metric["n_readings"],
                metric["avg_temp_c"],
                metric["min_temp_c"],
                metric["max_temp_c"],
                int(bool(metric["out_of_range"])),
                metric["pane_timing"],
                pane_index,
                int(bool(metric["is_final"])),
                metric["emitted_at"],
                now,
            ),
        )
    connection.execute(
        "INSERT INTO pane_log "
        "(aggregate_id, pane_index, pane_timing, n_readings, received_at, applied) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            metric["aggregate_id"],
            pane_index,
            metric["pane_timing"],
            metric["n_readings"],
            now,
            int(applied),
        ),
    )
    connection.commit()
    return applied


def build_consumer(settings: Settings, *, group_id: str = "coldchain-sink-sqlite-v1") -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            # Se confirma a mano en poll_into_store, después del commit de SQLite: una caída
            # entre ambos reprocesa el lote (el upsert lo tolera) en vez de perder un pane.
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([settings.topic_metrics])
    return consumer


def poll_into_store(
    consumer: Consumer,
    connection: sqlite3.Connection,
    *,
    max_messages: int = 500,
    timeout_seconds: float = 1.0,
) -> dict[str, int]:
    applied = ignored = errors = 0
    max_n_readings = 0
    messages = consumer.consume(num_messages=max_messages, timeout=timeout_seconds)
    for message in messages:
        if message.error():
            if message.error().code() != KafkaError._PARTITION_EOF:
                errors += 1
            continue
        try:
            metric = json.loads(message.value().decode())
            if not isinstance(metric, dict):
                raise TypeError(f"el pane no es un objeto JSON: {type(metric).__name__}")
            max_n_readings = max(max_n_readings, metric.get("n_readings") or 0)
            if upsert_metric(connection, metric):
                applied += 1
            else:
                ignored += 1
                logger.info(
                    "pane ignorado (mas viejo que el guardado) aggregate_id=%s pane_index=%s",
                    metric.get("aggregate_id"),
                    metric.get("pane_index"),
                )
        except (ValueError, KeyError, TypeError, AttributeError) as error:
            # Un mensaje dañado (tombstone, JSON que no es objeto, campo faltante o nulo) se
            # descarta y el lote sigue: si lanzara, el consumidor caería sin confirmar offsets y
            # al reiniciarse releería el mismo mensaje en bucle. El rollback deshace la fila de
            # metrics_5min que upsert_metric pudo escribir antes de fallar en pane_log.
            connection.rollback()
            errors += 1
            logger.warning(
                "pane descartado particion=%s offset=%s motivo=%r",
                message.partition(),
                message.offset(),
                error,
            )
    if any(message.error() is None for message in messages):
        consumer.commit(asynchronous=False)
    return {
        "applied": applied,
        "ignored": ignored,
        "errors": errors,
        "max_n_readings": max_n_readings,
    }


def run(*, group_id: str = "coldchain-sink-sqlite-v1") -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_env()
    connection = open_store(sink_path(settings))
    consumer = build_consumer(settings, group_id=group_id)
    logger.info("consumidor de metricas iniciado, escribiendo en %s", sink_path(settings))
    try:
        while True:
            result = poll_into_store(consumer, connection, timeout_seconds=1.0)
            if result["applied"] or result["ignored"] or result["errors"]:
                logger.info("panes procesados %s", result)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        connection.close()


if __name__ == "__main__":
    run()
