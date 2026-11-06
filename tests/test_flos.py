"""alice and the dispatch table: the request's Tuning TypeName picks the
FLO, and alice's answer keeps the recommendation's axioms."""

from __future__ import annotations

import pytest

from gwflo.flos import FLOS, alice, run
from gwflo.sema.enums import FloRefusalReason
from gwflo.sema.types import (
    FloRecommendation,
    FloRefusal,
    FloTuningAlice,
    FloTuningBruce,
)
from tests.conftest import (
    alice_request,
    bruce_can_build,
    bruce_installed,
    oak_bruce_request,
    sample,
)


def test_alice_buys_nothing_at_the_ceiling() -> None:
    request = alice_request()
    reply = alice(request)
    assert reply.bidder_alias == request.g_node_alias
    (pair,) = reply.pq_pairs
    assert pair.price_x1000 == round(request.price_max_usd_mwh * 1000)
    assert pair.quantity_x1000 == 0
    (path,) = reply.optimal_path_list
    assert path.start_unix_s == request.start_unix_s
    assert path.hp_avg_w_list[0] == 0
    assert len(path.hp_avg_w_list) == len(request.step_duration_minutes)
    assert reply.flo_name == "alice"


def test_alice_refuses_another_tuning() -> None:
    request = alice_request().model_copy(
        update={"tuning": FloTuningBruce.from_dict(sample("flo.tuning.bruce.001.json"))}
    )
    with pytest.raises(TypeError):
        alice(request)


def test_dispatch_is_by_tuning_type_name() -> None:
    assert set(FLOS) == {
        FloTuningAlice.type_name_value(),
        FloTuningBruce.type_name_value(),
    }
    reply = FloRecommendation.from_bytes(run(alice_request().to_bytes()))
    assert reply.flo_name == "alice"


def test_a_tuning_no_flo_answers_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delitem(FLOS, FloTuningAlice.type_name_value())
    request = alice_request()
    answer = FloRefusal.from_bytes(run(request.to_bytes()))
    assert answer.reason == FloRefusalReason.NoTuning
    assert answer.bidder_alias == request.g_node_alias
    assert answer.start_unix_s == request.start_unix_s
    assert answer.detail and FloTuningAlice.type_name_value() in answer.detail


def test_a_plant_shape_with_no_matrix_is_refused() -> None:
    """bruce installed, oak's shape, no cached matrix here: the worker
    answers NoMatrix rather than raising."""
    request = oak_bruce_request()
    if not bruce_installed():
        pytest.skip("no bruce extra")
    if bruce_can_build(request):
        pytest.skip("this laptop caches oak's matrix, so bruce would build")
    answer = FloRefusal.from_bytes(run(request.to_bytes()))
    assert answer.reason == FloRefusalReason.NoMatrix
    assert answer.detail and "transition matrix" in answer.detail
