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
