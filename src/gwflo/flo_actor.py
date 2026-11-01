"""FloActor: the service's broker face.

It hears every `flo.params` addressed to the flo class on the MQTT seam
(`amq.topic`, routing key `gw.<ltn>.to.flo.flo-params`), hands the
request's bytes to the build pool, and when the build returns publishes
the `flo.recommendation` back across the seam as a wrapped message
(`gw.<flo>.to.ltn.flo-recommendation`). The LTN pairs a reply with its
request by BidderAlias and the first path's StartUnixS, so no correlation
id travels. A reply older than the deadline is dropped: the LTN has bid
without it. Nothing is kept between requests.
"""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import Future
from concurrent.futures.process import BrokenProcessPool

from gwbase.orchestrator import Orchestrator
from gwbase.sema.types.gridworks_header import GridworksHeader
from gwbase.transport_encoding import RoutingEnvelope, TransportClass, routing_code
from gwbase.wrapped import wrap_bytes

from gwflo.config import GwfloSettings
from gwflo.pool import BuildPool
from gwflo.sema.types import FloParams, FloRecommendation

LOGGER = logging.getLogger(__name__)

REQUEST_TYPE = FloParams.type_name_value()
REPLY_TYPE = FloRecommendation.type_name_value()


def request_binding_key() -> str:
    """Every wrapped request addressed to the flo class, whoever sends it."""
    return (
        f"gw.*.to.{routing_code(TransportClass.Flo)}.{REQUEST_TYPE.replace('.', '-')}"
    )


class FloActor(Orchestrator):
    def __init__(self, *, settings: GwfloSettings, pool: BuildPool) -> None:
        super().__init__(
            settings=settings,
            transport_class=TransportClass.Flo,
            my_super_alias=settings.super_alias,
            my_time_coordinator_alias=settings.time_coordinator_alias,
        )
        self.reply_deadline_s = settings.reply_deadline_s
        self.pool = pool
        self.answered = 0
        self.dropped = 0

    def local_rabbit_startup(self) -> None:
        self.subscribe_amq_topic(binding_key=request_binding_key())

    def local_stop(self) -> None:
        super().local_stop()
        self.pool.shutdown()

    def process_message(
        self,
        *,
        envelope: RoutingEnvelope,
        body: bytes,
        header: GridworksHeader | None,  # noqa: ARG002 -- the header is not read
    ) -> None:
        if envelope.type_name != REQUEST_TYPE:
            return
        try:
            request = FloParams.from_bytes(body)
        except Exception as e:  # noqa: BLE001 -- the live path keeps running
            self.logger.error(f"Dropped {REQUEST_TYPE} from {envelope.from_alias}: {e}")
            return
        received_s = time.time()
        self.logger.info(
            f"Request from {request.g_node_alias} for slot {request.start_unix_s}: "
            f"{request.tuning.type_name}"
        )
        future = self.pool.submit(request.to_bytes())
        future.add_done_callback(lambda f: self.on_built(request, received_s, f))

    def on_built(
        self, request: FloParams, received_s: float, future: Future[bytes]
    ) -> None:
        try:
            reply_bytes = future.result()
        except BrokenProcessPool:
            self.logger.error(
                f"Build for {request.g_node_alias} slot {request.start_unix_s} lost: "
                "worker died"
            )
            return
        except Exception as e:  # noqa: BLE001 -- a refused request is reported, not fatal
            self.logger.error(
                f"Build for {request.g_node_alias} slot {request.start_unix_s} refused: {e}"
            )
            return
        age_s = time.time() - received_s
        if age_s > self.reply_deadline_s:
            self.dropped += 1
            self.logger.warning(
                f"Reply for {request.g_node_alias} slot {request.start_unix_s} dropped: "
                f"{age_s:.0f} s old, deadline {self.reply_deadline_s:.0f} s"
            )
            return
        self.reply(request, reply_bytes, age_s)

    def reply(self, request: FloParams, reply_bytes: bytes, age_s: float) -> None:
        payload = json.loads(reply_bytes)
        envelope = self.wrapped_envelope(
            type_name=REPLY_TYPE, to_class=TransportClass.LeafTransactiveNode
        )
        body = wrap_bytes(
            src=self.alias,
            dst=request.g_node_alias,
            inner_type_name=REPLY_TYPE,
            inner_payload_dict=payload,
        )
        self.send(envelope=envelope, body=body)
        self.answered += 1
        self.logger.info(
            f"Answered {request.g_node_alias} slot {request.start_unix_s} in {age_s:.1f} s: "
            f"{len(payload['PqPairs'])} pairs from {payload['FloName']}"
        )
