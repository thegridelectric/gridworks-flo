"""The FLOs this service answers with, keyed by the request's Tuning
TypeName, and the one function a worker runs.

A FLO is a callable from a `flo.params` to a `flo.recommendation`. The
service knows them by the table below and by nothing else: dispatch reads
the Tuning word's TypeName and the FLO does the rest. Alice lives here;
bruce is the optimizer in the sibling `bruce` package, reached through its
one boundary callable.
"""

from __future__ import annotations

import time
from collections.abc import Callable

from gwflo.sema.enums import MarketPriceUnit, MarketQuantityUnit
from gwflo.sema.types import (
    FloOptimalPath,
    FloParams,
    FloRecommendation,
    FloTuningAlice,
    FloTuningBruce,
    PriceQuantityUnitless,
)

Flo = Callable[[FloParams], FloRecommendation]

ALICE_NAME = "alice"
ALICE_REPO = "github.com/thegridelectric/gridworks-flo"
ALICE_COMMIT = "0" * 40


def alice(request: FloParams) -> FloRecommendation:
    """The FLO that buys nothing: one pair at the market ceiling for zero
    quantity, and beside it one path of zero heat pump power over the
    request's steps. Alice models no store, so the path's store line is
    zero watt-hours throughout."""
    if not isinstance(request.tuning, FloTuningAlice):
        raise TypeError(
            f"alice answers flo.tuning.alice, not {request.tuning.type_name}"
        )
    steps = len(request.step_duration_minutes)
    return FloRecommendation(
        bidder_alias=request.g_node_alias,
        pq_pairs=[
            PriceQuantityUnitless(
                price_x1000=round(request.price_max_usd_mwh * 1000),
                quantity_x1000=0,
            )
        ],
        price_unit=MarketPriceUnit.USDPerMWh,
        quantity_unit=MarketQuantityUnit.AvgkW,
        optimal_path_list=[
            FloOptimalPath(
                start_unix_s=request.start_unix_s,
                step_duration_minutes=list(request.step_duration_minutes),
                hp_avg_w_list=[0] * steps,
                store_wh_list=[0] * steps,
            )
        ],
        flo_name=ALICE_NAME,
        flo_repo=ALICE_REPO,
        flo_git_commit=ALICE_COMMIT,
        created_ms=int(time.time() * 1000),
    )


def bruce(request: FloParams) -> FloRecommendation:
    """The bruce optimizer, through its boundary callable. The two repos
    share the vocabulary and no Python types: the request crosses as
    bytes and the reply comes back as bytes."""
    # Imported here, not at module top: bruce (numpy, pandas, the graph)
    # is loaded only in the worker process that builds, so the actor stays
    # slim and a deploy without the bruce extra still answers alice.
    from bruce.service import recommend_bytes  # noqa: PLC0415  # pyright: ignore[reportMissingImports]

    return FloRecommendation.from_bytes(recommend_bytes(request.to_bytes()))


FLOS: dict[str, Flo] = {
    FloTuningAlice.type_name_value(): alice,
    FloTuningBruce.type_name_value(): bruce,
}


def run(request_bytes: bytes) -> bytes:
    """What a worker runs: decode the request, dispatch on its Tuning
    TypeName, encode the reply. Bytes at both ends so the pool moves
    nothing but JSON between processes."""
    request = FloParams.from_bytes(request_bytes)
    flo = FLOS[request.tuning.type_name]
    return flo(request).to_bytes()
