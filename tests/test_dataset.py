"""Carga de la fixture real y recorte por horas. Sin red ni Docker."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from coldchain_streaming.dataset import (
    _parse_elapsed_seconds,
    build_manifest,
    keep_prepared_at,
    load_raw,
    prepare_slice,
)

FIXTURE = Path(__file__).parent / "fixtures" / "test1_sample.csv"


@pytest.fixture
def raw_frame() -> pd.DataFrame:
    return load_raw(FIXTURE)


def test_fixture_has_91_rows_and_20_pod_columns(raw_frame):
    """La fixture real conserva sus 91 filas y 20 pods."""
    assert len(raw_frame) == 91
    pod_columns = [c for c in raw_frame.columns if c.startswith("Pod")]
    assert len(pod_columns) == 20


def test_pods_are_mapped_by_name_not_position(raw_frame):
    """El header trae los pods desordenados (Pod20...Pod1, Pod2 al final)."""
    events = prepare_slice(raw_frame, start_hours=0, end_hours=1)
    first_row = events[events["row_number"] == 0].set_index("sensor_id")
    # Pod9 (columna b9) y Pod13 deben identificarse por su nombre de columna.
    assert "ULT1-b09" in first_row.index
    assert "ULT1-b13" in first_row.index
    raw_pod9 = raw_frame.loc[0, "Pod9"]
    raw_pod13 = raw_frame.loc[0, "Pod13"]
    assert first_row.loc["ULT1-b09", "temp_f"] == pytest.approx(raw_pod9)
    assert first_row.loc["ULT1-b13", "temp_f"] == pytest.approx(raw_pod13)


def test_time_elapsed_parses_past_24_hours():
    """Time Elapsed supera las 24 h en el ensayo real."""
    assert _parse_elapsed_seconds("96:59:57") == 96 * 3600 + 59 * 60 + 57


def test_empty_or_non_numeric_cell_becomes_null_temp(raw_frame):
    """La celda vacía sale con temp nulo; valida Beam, no el productor."""
    corrupted = raw_frame.copy()
    corrupted["Pod8"] = corrupted["Pod8"].astype(object)
    corrupted.loc[0, "Pod7"] = float("nan")
    corrupted.loc[0, "Pod8"] = "abc"
    events = prepare_slice(corrupted, start_hours=0, end_hours=1)
    for sensor_id in ("ULT1-b07", "ULT1-b08"):
        row = events[(events["row_number"] == 0) & (events["sensor_id"] == sensor_id)].iloc[0]
        assert pd.isna(row["temp_c"]), sensor_id
        assert pd.isna(row["temp_f"]), sensor_id
    # el resto de la fila sigue intacto
    ok = events[(events["row_number"] == 0) & (events["sensor_id"] == "ULT1-b09")].iloc[0]
    assert ok["temp_f"] == pytest.approx(raw_frame.loc[0, "Pod9"])


def test_slice_by_hours_filters_rows(raw_frame):
    """El recorte por horas deja solo el tramo pedido."""
    events = prepare_slice(raw_frame, start_hours=0, end_hours=0.1)  # 6 minutos
    assert events["elapsed_s"].max() < 360
    assert len(events) < 91 * 20


def test_event_count_is_20_per_row(raw_frame):
    """Un evento por (fila, pod)."""
    events = prepare_slice(raw_frame, start_hours=0, end_hours=1)
    assert len(events) == 91 * 20
    assert events["row_number"].nunique() == 91


def test_manifest_has_expected_shape(raw_frame):
    """El manifest describe el tramo de forma reproducible."""
    events = prepare_slice(raw_frame, start_hours=0, end_hours=1)
    manifest = build_manifest(events, start_hours=0, end_hours=1)
    assert manifest["rows"] == 91
    assert manifest["events"] == 91 * 20
    assert manifest["sensors"] == 20
    assert manifest["source_md5"] == "b8164462feaca1d699a53a2503f92d30"
    assert manifest["first_source_time"] < manifest["last_source_time"]


def test_manifest_keeps_prepared_at_when_the_slice_did_not_change(raw_frame):
    """Rehacer el mismo tramo no ensucia el manifest versionado."""
    events = prepare_slice(raw_frame, start_hours=0, end_hours=1)
    previous = {**build_manifest(events, start_hours=0, end_hours=1), "prepared_at": "antes"}
    manifest = keep_prepared_at(build_manifest(events, start_hours=0, end_hours=1), previous)
    assert manifest["prepared_at"] == "antes"


def test_manifest_gets_a_new_prepared_at_when_the_slice_changes(raw_frame):
    """Un tramo distinto sí actualiza la fecha de preparación."""
    events = prepare_slice(raw_frame, start_hours=0, end_hours=1)
    previous = {**build_manifest(events, start_hours=0, end_hours=1), "prepared_at": "antes"}
    shorter = prepare_slice(raw_frame, start_hours=0, end_hours=0.1)
    manifest = keep_prepared_at(build_manifest(shorter, start_hours=0, end_hours=0.1), previous)
    assert manifest["prepared_at"] != "antes"
