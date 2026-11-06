"""Against the real dev broker, both ways an LTN reaches the FLO: a
flo.params sent direct over the LeafTransactiveNode-to-Flo edge comes
back as a flo.recommendation direct on the LTN's queue, and one published
wrapped on the MQTT door comes back wrapped on the topic an MQTT-native
LTN subscribes to. Self-skips without a broker."""

from __future__ import annotations

import json
import time
from queue import Empty, Queue

import paho.mqtt.client as mqtt
import pika
import pytest
from gwbase.transport_encoding import (
    MessageCategory,
    TransportClass,
    json_direct_routing_key,
    routing_code,
)
from gwbase.wrapped import wrap_bytes
from paho.mqtt.enums import CallbackAPIVersion

from gwflo.config import GwfloSettings
from gwflo.flo_actor import FloActor
from gwflo.pool import BuildPool
from gwflo.sema.types import FloParams
from tests.conftest import (
    alice_request,
    broker_up,
    bruce_can_build,
    bruce_installed,
    oak_bruce_request,
)

MQTT_PORT = 1885
AMQP_URL = "amqp://smqPublic:smqPublic@localhost:5672/d1__1"


def wait_for(predicate, timeout_s: float, what: str) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}")


def round_trip(
    settings: GwfloSettings,
    request: FloParams,
    timeout_s: float,
    expect: str = "flo.recommendation",
) -> dict:
    """Publish the request on the MQTT door as the LTN would and return the
    answer's payload, of the expected type, that comes back on the LTN's
    topic for that type."""
    ltn_lrh = request.g_node_alias.replace(".", "-")
    flo_lrh = settings.service_alias.replace(".", "-")
    replies: Queue[bytes] = Queue()

    client = mqtt.Client(CallbackAPIVersion.VERSION2, client_id=f"test-ltn-{ltn_lrh}")
    client.username_pw_set("smqPublic", "smqPublic")
    client.on_message = lambda c, u, m: replies.put(m.payload)
    client.connect("localhost", MQTT_PORT)
    client.subscribe(f"gw/{flo_lrh}/to/ltn/{expect.replace('.', '-')}")
    client.loop_start()

    actor = FloActor(settings=settings, pool=BuildPool(1, timeout_s=60))
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
            raise AssertionError(f"no {expect} came back over MQTT") from None
        assert reply["TypeName"] == "gw"
        assert reply["Header"]["Src"] == settings.service_alias
        assert reply["Header"]["Dst"] == request.g_node_alias
        payload = reply["Payload"]
        assert payload["TypeName"] == expect
        assert payload["BidderAlias"] == request.g_node_alias
        return payload
    finally:
        actor.stop()
        client.loop_stop()
        client.disconnect()


def round_trip_direct(
    settings: GwfloSettings, request: FloParams, timeout_s: float
) -> dict:
    """Send the request direct on the LTN's mic exchange, as a gwbase LTN
    does, and return the flo.recommendation that reaches the LTN's queue
    on its class exchange."""
    ltn = request.g_node_alias
    ltn_code = routing_code(TransportClass.LeafTransactiveNode)
    flo_code = routing_code(TransportClass.Flo)
    connection = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
    channel = connection.channel()
    queue = channel.queue_declare(queue="", exclusive=True).method.queue
    channel.queue_bind(
        queue,
        f"{ltn_code}_tx",
        routing_key=f"{MessageCategory.JsonDirect.value}.*.*.*.*.{ltn.replace('.', '-')}",
    )
    actor = FloActor(settings=settings, pool=BuildPool(1, timeout_s=60))
    actor.start()
    try:
        wait_for(lambda: actor.consuming, 15, "the actor to consume")
        channel.basic_publish(
            exchange=f"{ltn_code}mic_tx",
            routing_key=json_direct_routing_key(
                from_alias=ltn,
                from_class_token=ltn_code,
                type_name="flo.params",
                to_class_token=flo_code,
                to_alias=settings.service_alias,
            ),
            body=request.to_bytes(),
        )
        deadline = time.time() + timeout_s
        method = body = None
        while method is None:
            if time.time() > deadline:
                raise AssertionError("no flo.recommendation came back on the edge")
            method, _properties, body = channel.basic_get(queue, auto_ack=True)
            if method is None:
                time.sleep(0.2)
        assert body is not None
        assert method.routing_key == json_direct_routing_key(
            from_alias=settings.service_alias,
            from_class_token=flo_code,
            type_name="flo.recommendation",
            to_class_token=ltn_code,
            to_alias=ltn,
        )
        payload = json.loads(body)
        assert payload["TypeName"] == "flo.recommendation"
        assert payload["BidderAlias"] == ltn
        assert payload["OptimalPathList"][0]["StartUnixS"] == request.start_unix_s
        assert actor.answered == 1
        return payload
    finally:
        actor.stop()
        connection.close()


@pytest.mark.broker
def test_alice_request_over_the_edge_is_answered(settings: GwfloSettings) -> None:
    if not broker_up(5672):
        pytest.skip("no dev broker on localhost:5672")
    payload = round_trip_direct(settings, alice_request(), timeout_s=30)
    assert payload["FloName"] == "alice"


@pytest.mark.broker
def test_alice_request_over_mqtt_is_answered(settings: GwfloSettings) -> None:
    if not (broker_up(5672) and broker_up(MQTT_PORT)):
        pytest.skip("no dev broker on localhost:5672 + 1885")
    payload = round_trip(settings, alice_request(), timeout_s=30)
    assert payload["FloName"] == "alice"
    assert payload["OptimalPathList"][0]["StartUnixS"] == alice_request().start_unix_s


@pytest.mark.broker
def test_a_request_the_flo_cannot_serve_is_refused_over_mqtt(
    settings: GwfloSettings,
) -> None:
    """oak's plant shape with no cached matrix here: a flo.refusal comes
    back wrapped on the LTN's topic, in place of the recommendation."""
    if not (broker_up(5672) and broker_up(MQTT_PORT)):
        pytest.skip("no dev broker on localhost:5672 + 1885")
    request = oak_bruce_request()
    if not bruce_installed():
        pytest.skip("no bruce extra")
    if bruce_can_build(request):
        pytest.skip("this laptop caches oak's matrix, so bruce would build")
    payload = round_trip(settings, request, timeout_s=60, expect="flo.refusal")
    assert payload["Reason"] == "NoMatrix"
    assert payload["StartUnixS"] == request.start_unix_s


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
