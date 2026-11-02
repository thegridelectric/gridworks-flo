"""Shared fixtures: the actor writes logs under XDG dirs, so tests point
those at a temp dir before any settings or actor exists."""

from __future__ import annotations

import json
import socket
from pathlib import Path

import pytest

from gwflo.config import GwfloSettings
from gwflo.sema.types import FloParams, FloTuningAlice

SAMPLES = Path(__file__).resolve().parents[1] / "src" / "gwflo" / "sema" / "samples"


@pytest.fixture(autouse=True)
def xdg_tmp(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    for var in ("XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME"):
        monkeypatch.setenv(var, str(tmp_path / var.lower()))
    monkeypatch.setenv("GWFLO_SUPER_ALIAS", "d1.super1")
    monkeypatch.setenv("GWFLO_TIME_COORDINATOR_ALIAS", "d1.time")
    monkeypatch.setenv("GWFLO_WORKERS", "1")
    monkeypatch.setenv("GWFLO_REPLY_DEADLINE_S", "60")
    return tmp_path


def broker_up(port: int) -> bool:
    """Whether the gwbase dev broker answers on this localhost port."""
    try:
        with socket.create_connection(("localhost", port), timeout=1):
            return True
    except OSError:
        return False


@pytest.fixture
def settings() -> GwfloSettings:
    return GwfloSettings.load()


def sample(name: str) -> dict:
    return json.loads((SAMPLES / name).read_text())


def alice_request() -> FloParams:
    """The sample request, re-tuned to alice."""
    request = FloParams.from_dict(sample("flo.params.000.json"))
    return request.model_copy(update={"tuning": FloTuningAlice()})


def oak_bruce_request() -> FloParams:
    """The sample request at oak's live plant shape: one tank, nine
    sensors, 120 gallons, the shape whose transition matrix the laptop and
    the LTN boxes cache as ``heat_pump_water_tank_2b6a76e0``."""
    request = sample("flo.params.000.json")
    request.update(
        TempSensorsPerTank=9,
        MaxTankTempF=160.0,
        InitialThermocline1=3,
        InitialThermocline2=6,
    )
    return FloParams.from_dict(request)


def bruce_can_build(request: FloParams) -> bool:
    """True when the bruce extra is installed and the request's plant shape
    has a cached transition matrix here. The request crosses into bruce as
    bytes: the two packages share the vocabulary, not Python types."""
    try:
        from bruce.graph_optimizer.assets.heat_pump_water_tank.transition_matrix_params import (  # noqa: PLC0415  # pyright: ignore[reportMissingImports]
            HeatPumpWaterTankTransitionMatrixParams,
        )
        from bruce.graph_optimizer.settings import config_dir  # noqa: PLC0415  # pyright: ignore[reportMissingImports]
        from bruce.sema.types import FloParams as BruceFloParams  # noqa: PLC0415  # pyright: ignore[reportMissingImports]
        from bruce.service import params_from_request  # noqa: PLC0415  # pyright: ignore[reportMissingImports]
    except ImportError:
        return False
    params = params_from_request(BruceFloParams.from_bytes(request.to_bytes()))
    matrix = HeatPumpWaterTankTransitionMatrixParams.from_asset_params(params)
    return (config_dir() / matrix.cache_filename("heat_pump_water_tank")).exists()
