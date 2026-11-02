"""Against the real dev broker, across the MQTT seam the LTN uses: a
flo.params published on the MQTT door comes back as a flo.recommendation
on the topic an LTN would subscribe to. Self-skips without a broker."""

from __future__ import annotations

import json
import time
from queue import Empty, Queue

import paho.mqtt.client as mqtt
import pytest
from gwbase.wrapped import wrap_bytes
from paho.mqtt.enums import CallbackAPIVersion

from gwflo.config import GwfloSettings
from gwflo.flo_actor import FloActor
from gwflo.pool import BuildPool
from gwflo.sema.types import FloParams
from tests.conftest import alice_request, broker_up, bruce_can_build, oak_bruce_request

MQTT_PORT = 1885


def wait_for(predicate, timeout_s: float, what: str) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}")


def round_trip(settings: GwfloSettings, request: FloParams, timeout_s: float) -> dict:
    """Publish the request on the MQTT door as the LTN would and return the
    flo.recommendation payload that comes back on the LTN's topic."""
    ltn_lrh = request.g_node_alias.replace(".", "-")
    flo_lrh = settings.service_alias.replace(".", "-")
    replies: Queue[bytes] = Queue()

    client = mqtt.Client(CallbackAPIVersion.VERSION2, client_id=f"test-ltn-{ltn_lrh}")
    client.username_pw_set("smqPublic", "smqPublic")
    client.on_message = lambda c, u, m: replies.put(m.payload)
    client.connect("localhost", MQTT_PORT)
    client.subscribe(f"gw/{flo_lrh}/to/ltn/flo-recommendation")
    client.loop_start()

    actor = FloActor(settings=settings, pool=BuildPool(1))
    actor.start()
    try:
        wait_for(lambda: actor.consuming, 15, "the actor to consume")
        body = wrap_bytes(
            src=request.g_node_alias,
            dst=settings.service_alias,
            inner_type_name="flo.params",
            inner_payload_dict=request.to_dict(),
        )
        client.publish(f"gw/{ltn_lrh}/to/flo/flo-params", body)
        try:
            reply = json.loads(replies.get(timeout=timeout_s))
        except Empty:
            raise AssertionError("no flo.recommendation came back over MQTT") from None
        assert reply["TypeName"] == "gw"
        assert reply["Header"]["Src"] == settings.service_alias
        assert reply["Header"]["Dst"] == request.g_node_alias
        payload = reply["Payload"]
        assert payload["TypeName"] == "flo.recommendation"
        assert payload["BidderAlias"] == request.g_node_alias
        assert payload["OptimalPathList"][0]["StartUnixS"] == request.start_unix_s
        assert actor.answered == 1
        return payload
    finally:
        actor.stop()
        client.loop_stop()
        client.disconnect()


@pytest.mark.broker
def test_alice_request_over_mqtt_is_answered(settings: GwfloSettings) -> None:
    if not (broker_up(5672) and broker_up(MQTT_PORT)):
        pytest.skip("no dev broker on localhost:5672 + 1885")
    payload = round_trip(settings, alice_request(), timeout_s=30)
    assert payload["FloName"] == "alice"


@pytest.mark.broker
def test_bruce_request_over_mqtt_is_answered(settings: GwfloSettings) -> None:
    """The same seam with the real optimizer behind it: oak's plant shape
    against its cached matrix, one pair per quantity change, one path per
    pair, the first step of each path the bid at that pair's price."""
    if not (broker_up(5672) and broker_up(MQTT_PORT)):
        pytest.skip("no dev broker on localhost:5672 + 1885")
    request = oak_bruce_request()
    if not bruce_can_build(request):
        pytest.skip("no bruce extra, or no cached matrix for oak's plant shape")
    payload = round_trip(settings, request, timeout_s=120)
    assert payload["FloName"] == "bruce"
    pairs = payload["PqPairs"]
    assert pairs[0]["PriceX1000"] == round(request.price_max_usd_mwh * 1000)
    assert len(pairs) >= 2
    assert len(payload["OptimalPathList"]) == len(pairs)
    assert [p["PriceX1000"] for p in pairs] == sorted(
        (p["PriceX1000"] for p in pairs), reverse=True
    )
    assert all(
        path["StepDurationMinutes"] == request.step_duration_minutes
        for path in payload["OptimalPathList"]
    )
