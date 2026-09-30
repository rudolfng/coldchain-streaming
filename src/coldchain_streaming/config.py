"""Configuración compartida por CLI, pipeline, pruebas y scripts."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

TOPIC_RAW = "coldchain.telemetry.raw.v1"
TOPIC_METRICS = "coldchain.metrics.5min.v1"
TOPIC_DLQ = "coldchain.telemetry.dlq.v1"

ASSET_ID = "ULT1"
ASSET_TYPE = "contenedor"
TEMP_MIN_C = -80.0
TEMP_MAX_C = -60.0


@dataclass(frozen=True)
class Settings:
    """Parámetros de ejecución con valores por defecto listos para Docker."""

    kafka_bootstrap_servers: str = "kafka:9092"
    topic_raw: str = TOPIC_RAW
    topic_metrics: str = TOPIC_METRICS
    topic_dlq: str = TOPIC_DLQ
    window_seconds: int = 300
    allowed_lateness_seconds: int = 900
    early_firing_seconds: int = 5
    parallelism: int = 2
    job_endpoint: str = "beam-job-server:8099"

    replay_start_hours: float = 65.0
    replay_end_hours: float = 68.0
    replay_speedup: float = 20.0
    replay_seed: int = 7
    replay_injections: str = "demo"
    replay_duplicate_rate: float = 0.02
    replay_invalid_rate: float = 0.005
    replay_align_start: bool = False

    sink_path: str = "data/out/metrics.sqlite"

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            topic_raw=os.getenv("TOPIC_RAW", TOPIC_RAW),
            topic_metrics=os.getenv("TOPIC_METRICS", TOPIC_METRICS),
            topic_dlq=os.getenv("TOPIC_DLQ", TOPIC_DLQ),
            window_seconds=int(os.getenv("WINDOW_SECONDS", "300")),
            allowed_lateness_seconds=int(os.getenv("ALLOWED_LATENESS_SECONDS", "900")),
            early_firing_seconds=int(os.getenv("EARLY_FIRING_SECONDS", "5")),
            parallelism=int(os.getenv("BEAM_PARALLELISM", "2")),
            job_endpoint=os.getenv("BEAM_JOB_ENDPOINT", "beam-job-server:8099"),
            replay_start_hours=float(os.getenv("REPLAY_START_HOURS", "65")),
            replay_end_hours=float(os.getenv("REPLAY_END_HOURS", "68")),
            replay_speedup=float(os.getenv("REPLAY_SPEEDUP", "20")),
            replay_seed=int(os.getenv("REPLAY_SEED", "7")),
            replay_injections=os.getenv("REPLAY_INJECTIONS", "demo"),
            replay_duplicate_rate=float(os.getenv("REPLAY_DUPLICATE_RATE", "0.02")),
            replay_invalid_rate=float(os.getenv("REPLAY_INVALID_RATE", "0.005")),
            replay_align_start=os.getenv("REPLAY_ALIGN_START", "0").strip().lower()
            in {"1", "true", "yes"},
            sink_path=os.getenv("SINK_PATH", "data/out/metrics.sqlite"),
        )


def project_root() -> Path:
    """Raíz del proyecto, tanto en el repo como dentro del contenedor (/app)."""
    configured = os.getenv("COLDCHAIN_ROOT")
    if configured:
        return Path(configured).resolve()
    return Path(__file__).resolve().parents[2]


def raw_csv_path() -> Path:
    default = project_root() / "data/cache/Test1_TempCO2O2.csv"
    return Path(os.getenv("COLDCHAIN_RAW_CSV", default))


def processed_dataset_path() -> Path:
    default = project_root() / "data/processed/telemetry_slice.parquet"
    return Path(os.getenv("COLDCHAIN_DATASET_PATH", default))


def manifest_path() -> Path:
    return processed_dataset_path().parent / "manifest.json"


def sink_path(settings: Settings | None = None) -> Path:
    configured = (settings or Settings.from_env()).sink_path
    path = Path(configured)
    if path.is_absolute():
        return path
    return project_root() / path
