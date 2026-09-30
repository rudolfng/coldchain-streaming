"""Descarga y prepara el recorte del ensayo Test1."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import shutil
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from coldchain_streaming.config import (
    Settings,
    manifest_path,
    processed_dataset_path,
    raw_csv_path,
)
from coldchain_streaming.contracts import pod_to_sensor_id, temp_f_to_c

DATASET_URL = "https://ndownloader.figshare.com/files/28668003"
EXPECTED_MD5 = "b8164462feaca1d699a53a2503f92d30"
SOURCE_TZ = ZoneInfo("America/New_York")
DEFAULT_START_HOURS = 65.0
DEFAULT_END_HOURS = 68.0
CLOCK_DRIFT_TOLERANCE_S = 60.0

POD_COLUMNS = [f"Pod{n}" for n in range(1, 21)]

logger = logging.getLogger("coldchain_streaming.dataset")


def md5sum(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(
    url: str = DATASET_URL, destination: Path | None = None, *, force: bool = False
) -> Path:
    """Descarga atómica (.part + rename); reusa el archivo si el md5 ya coincide.

    Si la descarga automática de figshare falla (a veces redirige), bajar el CSV a mano
    a `destination` (por defecto `data/cache/Test1_TempCO2O2.csv`); esta función solo exige
    que el md5 coincida.
    """
    destination = destination or raw_csv_path()
    if destination.exists() and not force and md5sum(destination) == EXPECTED_MD5:
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_suffix(destination.suffix + ".part")
    request = urllib.request.Request(url, headers={"User-Agent": "coldchain-streaming/1.0"})
    with urllib.request.urlopen(request, timeout=180) as response, partial.open("wb") as target:  # noqa: S310
        shutil.copyfileobj(response, target)
    partial.replace(destination)
    digest = md5sum(destination)
    if digest != EXPECTED_MD5:
        destination.unlink(missing_ok=True)
        raise ValueError(
            f"md5 no coincide tras la descarga ({digest} != {EXPECTED_MD5}); "
            f"descargar manualmente {url} a {destination}"
        )
    return destination


def _read_header(path: Path) -> list[str]:
    """Nombres de columna de la fila 1; las dos columnas finales sin nombre se numeran."""
    with path.open() as handle:
        names = handle.readline().rstrip("\n").split(",")
    seen: set[str] = set()
    unique = []
    for index, name in enumerate(names):
        if not name or name in seen:
            name = f"_unnamed_{index}"
        seen.add(name)
        unique.append(name)
    return unique


def load_raw(path: Path) -> pd.DataFrame:
    """Lee el CSV real: 3 filas de encabezado, columnas por nombre (nunca por posición)."""
    names = _read_header(path)
    frame = pd.read_csv(path, header=None, skiprows=3, names=names, low_memory=False)
    return frame[["date", "time", "Time Elapsed", *POD_COLUMNS]]


def _parse_elapsed_seconds(value: str) -> int:
    hours, minutes, seconds = value.split(":")
    return int(hours) * 3600 + int(minutes) * 60 + int(seconds)


def _parse_source_time(date: str, time: str) -> datetime:
    """Hora local del laboratorio (ORNL, America/New_York), convertida a UTC."""
    local = datetime.strptime(f"{date} {time}", "%d-%b-%y %H:%M:%S").replace(tzinfo=SOURCE_TZ)
    return local.astimezone(UTC)


def prepare_slice(
    frame: pd.DataFrame,
    *,
    start_hours: float = DEFAULT_START_HOURS,
    end_hours: float = DEFAULT_END_HOURS,
) -> pd.DataFrame:
    """Recorta por `Time Elapsed` y devuelve un evento por (fila, pod).

    Celda vacía o no numérica -> el evento se emite igual con `temp_c`/`temp_f` en None:
    el productor no filtra, validar es trabajo de Beam.
    """
    working = frame.copy()
    # Texto u otra basura en una celda de temperatura -> NaN, igual que una celda vacía.
    working[POD_COLUMNS] = working[POD_COLUMNS].apply(pd.to_numeric, errors="coerce")
    working["elapsed_s"] = working["Time Elapsed"].map(_parse_elapsed_seconds)
    working["source_time"] = [
        _parse_source_time(date, time)
        for date, time in zip(working["date"], working["time"], strict=True)
    ]

    first_source_time = working["source_time"].iloc[0]
    first_elapsed_s = working["elapsed_s"].iloc[0]
    drift = (working["source_time"] - first_source_time).dt.total_seconds() - (
        working["elapsed_s"] - first_elapsed_s
    )
    if drift.abs().max() > CLOCK_DRIFT_TOLERANCE_S:
        logger.warning(
            "elapsed_s y source_time difieren mas de %.0fs (maximo observado %.1fs); posible "
            "corte de reloj. Se sigue usando elapsed_s.",
            CLOCK_DRIFT_TOLERANCE_S,
            drift.abs().max(),
        )

    start_s = start_hours * 3600
    end_s = end_hours * 3600
    sliced = working[
        (working["elapsed_s"] >= start_s) & (working["elapsed_s"] < end_s)
    ].reset_index(drop=True)
    sliced = sliced.assign(row_number=sliced.index)

    events = sliced.melt(
        id_vars=["source_time", "elapsed_s", "row_number"],
        value_vars=POD_COLUMNS,
        var_name="pod",
        value_name="temp_f",
    )
    events["sensor_id"] = events["pod"].map(pod_to_sensor_id)
    events["temp_c"] = events["temp_f"].map(
        lambda value: temp_f_to_c(value) if pd.notna(value) else float("nan")
    )
    # temp_f/temp_c quedan en NaN (no en None) para poder escribirse en parquet como float64;
    # el productor los convierte a `null` de JSON al construir el evento.
    events = events.sort_values(["elapsed_s", "sensor_id"], kind="stable").reset_index(drop=True)
    return events[["source_time", "elapsed_s", "sensor_id", "temp_f", "temp_c", "row_number"]]


def build_manifest(
    events: pd.DataFrame, *, start_hours: float, end_hours: float
) -> dict[str, object]:
    """Resumen reproducible del tramo: URL, md5, filas, eventos, rango temporal."""
    return {
        "source_url": DATASET_URL,
        "source_md5": EXPECTED_MD5,
        "start_hours": start_hours,
        "end_hours": end_hours,
        "rows": int(events["row_number"].nunique()),
        "events": int(len(events)),
        "sensors": int(events["sensor_id"].nunique()),
        "first_source_time": events["source_time"].min().isoformat(),
        "last_source_time": events["source_time"].max().isoformat(),
        "prepared_at": datetime.now(UTC).isoformat(),
    }


def keep_prepared_at(
    manifest: dict[str, object], previous: dict[str, object] | None
) -> dict[str, object]:
    """Conserva el `prepared_at` anterior si el resto del manifest no cambió.

    `dataset-init` corre en cada `make up` y `make smoke`; sin esto reescribiría el
    `manifest.json` versionado con una fecha nueva aunque el tramo sea el mismo.
    """
    if previous is None or "prepared_at" not in previous:
        return manifest
    same = {k: v for k, v in previous.items() if k != "prepared_at"} == {
        k: v for k, v in manifest.items() if k != "prepared_at"
    }
    return {**manifest, "prepared_at": previous["prepared_at"]} if same else manifest


def prepare_dataset(
    *,
    start_hours: float = DEFAULT_START_HOURS,
    end_hours: float = DEFAULT_END_HOURS,
    force_download: bool = False,
) -> dict[str, object]:
    """Descarga, recorta y escribe el parquet y el manifest del tramo elegido."""
    csv_path = download(force=force_download)
    frame = load_raw(csv_path)
    events = prepare_slice(frame, start_hours=start_hours, end_hours=end_hours)
    if events.empty:
        raise RuntimeError(
            "el tramo elegido no contiene eventos; ajustar --start-hours/--end-hours"
        )

    output = processed_dataset_path()
    output.parent.mkdir(parents=True, exist_ok=True)
    events.to_parquet(output, index=False)

    manifest = build_manifest(events, start_hours=start_hours, end_hours=end_hours)
    previous = json.loads(manifest_path().read_text()) if manifest_path().exists() else None
    manifest = keep_prepared_at(manifest, previous)
    manifest_path().write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_env()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-hours", type=float, default=settings.replay_start_hours)
    parser.add_argument("--end-hours", type=float, default=settings.replay_end_hours)
    parser.add_argument("--force-download", action="store_true")
    args = parser.parse_args()
    print(
        json.dumps(
            prepare_dataset(
                start_hours=args.start_hours,
                end_hours=args.end_hours,
                force_download=args.force_download,
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
