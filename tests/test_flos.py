"""alice and the dispatch table: the request's Tuning TypeName picks the
FLO, and alice's answer keeps the recommendation's axioms."""

from __future__ import annotations

import pytest

from gwflo.flos import FLOS, alice, run
from gwflo.sema.types import FloRecommendation, FloTuningAlice, FloTuningBruce
from tests.conftest import alice_request, sample


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
