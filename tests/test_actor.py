"""The actor's answer to each way a build ends, with the broker stood in
for by a captured send: a recommendation in time is answered, one past
the deadline is refused, and a lost build, a build that raised and a
worker's own refusal each go back as a flo.refusal in the kind the
request came."""

from __future__ import annotations

import json
import time
from concurrent.futures import Future

import pytest
from gwbase.wrapped import unwrap_bytes

from gwflo.config import GwfloSettings
from gwflo.flo_actor import FloActor
from gwflo.flos import alice, refusal
from gwflo.pool import BuildLost, BuildPool, BuildRefused
from gwflo.sema.enums import FloRefusalReason
from gwflo.sema.types import FloRefusal
from tests.conftest import alice_request


class Captured:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes]] = []

    def send(self, *, envelope, body: bytes) -> None:  # noqa: ANN001
        self.sent.append((envelope.type_name, body))


@pytest.fixture
def actor(settings: GwfloSettings, monkeypatch: pytest.MonkeyPatch) -> FloActor:
    actor = FloActor(settings=settings, pool=BuildPool(1, timeout_s=60))
    captured = Captured()
    monkeypatch.setattr(actor, "send", captured.send)
    actor.captured = captured  # pyright: ignore[reportAttributeAccessIssue]
    return actor


def done(result: bytes | None = None, error: Exception | None = None) -> Future[bytes]:
    future: Future[bytes] = Future()
    if error is not None:
        future.set_exception(error)
    else:
        assert result is not None
        future.set_result(result)
    return future


def sent(actor: FloActor) -> list[tuple[str, bytes]]:
    return actor.captured.sent  # pyright: ignore[reportAttributeAccessIssue]


def test_a_recommendation_in_time_is_answered_direct(actor: FloActor) -> None:
    request = alice_request()
    actor.on_built(request, time.time(), done(alice(request).to_bytes()), wrapped=False)
    ((type_name, body),) = sent(actor)
    assert type_name == "flo.recommendation"
    assert json.loads(body)["BidderAlias"] == request.g_node_alias
    assert (actor.answered, actor.refused) == (1, 0)


def test_a_recommendation_past_the_deadline_is_refused(actor: FloActor) -> None:
    request = alice_request()
    received_s = time.time() - actor.reply_deadline_s - 1
    actor.on_built(request, received_s, done(alice(request).to_bytes()), wrapped=False)
    ((type_name, body),) = sent(actor)
    assert type_name == "flo.refusal"
    answer = FloRefusal.from_bytes(body)
    assert answer.reason == FloRefusalReason.PastDeadline
    assert answer.start_unix_s == request.start_unix_s
    assert (actor.answered, actor.refused) == (0, 1)


def test_a_lost_build_is_refused_wrapped(actor: FloActor) -> None:
    request = alice_request()
    actor.on_built(
        request,
        time.time(),
        done(error=BuildLost("worker killed by signal 9")),
        wrapped=True,
    )
    ((type_name, body),) = sent(actor)
    assert type_name == "flo.refusal"
    header, payload = unwrap_bytes(body)
    assert header.dst == request.g_node_alias
    answer = FloRefusal.from_dict(payload)
    assert answer.reason == FloRefusalReason.BuildLost
    assert answer.detail == "worker killed by signal 9"


def test_a_build_that_raised_is_refused_with_its_last_line(actor: FloActor) -> None:
    request = alice_request()
    stderr = (
        "Traceback (most recent call last):\n  File x, line 1\nValueError: bad tank\n"
    )
    actor.on_built(
        request, time.time(), done(error=BuildRefused(stderr)), wrapped=False
    )
    ((_, body),) = sent(actor)
    answer = FloRefusal.from_bytes(body)
    assert answer.reason == FloRefusalReason.BuildFailed
    assert answer.detail == "ValueError: bad tank"


def test_a_workers_refusal_is_passed_on(actor: FloActor) -> None:
    request = alice_request()
    worker_answer = refusal(request, FloRefusalReason.NoMatrix, "no matrix").to_bytes()
    actor.on_built(request, time.time(), done(worker_answer), wrapped=False)
    ((type_name, body),) = sent(actor)
    assert type_name == "flo.refusal"
    assert body == worker_answer
    assert actor.refused == 1
