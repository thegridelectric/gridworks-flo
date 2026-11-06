"""FloActor: the service's broker face.

It hears `flo.params` two ways and answers each in kind. A gwbase LTN
sends it direct over the LeafTransactiveNode-to-Flo edge and gets the
`flo.recommendation` direct over the edge back. An LTN still inside
gridworks-scada is an MQTT client: its request arrives wrapped on
`amq.topic` (`gw.<ltn>.to.flo.flo-params`) and its reply goes back
wrapped (`gw.<flo>.to.ltn.flo-recommendation`). The `amq.topic` binding
goes once the LTN has fully moved out of gridworks-scada.

The request's bytes go to the build pool, and when the build returns the
reply is published. The LTN pairs a reply with its request by
BidderAlias and the first path's StartUnixS, so no correlation id
travels. A reply older than the deadline is dropped: the LTN has bid
without it. Nothing is kept between requests.

The actor is a GNode: its `g.node.gt.json` names the registry row an LTN
finds by walking the forest from itself up to the universe root.
"""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import Future

from gwbase.gridworks_actor import GridworksActor
from gwbase.sema.enums import GwGNodeClass
from gwbase.sema.types.gridworks_header import GridworksHeader
from gwbase.transport_encoding import RoutingEnvelope, TransportClass, routing_code
from gwbase.wrapped import wrap_bytes

from gwflo.config import GwfloSettings
from gwflo.pool import BuildLost, BuildPool, BuildRefused
from gwflo.sema.types import FloParams, FloRecommendation

LOGGER = logging.getLogger(__name__)

REQUEST_TYPE = FloParams.type_name_value()
REPLY_TYPE = FloRecommendation.type_name_value()


def request_binding_key() -> str:
    """Every wrapped request addressed to the flo class, whoever sends it:
    the MQTT-native LTN's path, bound on `amq.topic` until the LTN has
    fully moved out of gridworks-scada."""
    return (
        f"gw.*.to.{routing_code(TransportClass.Flo)}.{REQUEST_TYPE.replace('.', '-')}"
    )


class FloActor(GridworksActor):
    def __init__(self, *, settings: GwfloSettings, pool: BuildPool) -> None:
        super().__init__(
            settings=settings,
            g_node_class=GwGNodeClass.Flo,
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
        header: GridworksHeader | None,
    ) -> None:
        if envelope.type_name != REQUEST_TYPE:
            return
        wrapped = header is not None
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
        future.add_done_callback(
            lambda f: self.on_built(request, received_s, f, wrapped=wrapped)
        )

    def on_built(
        self,
        request: FloParams,
        received_s: float,
        future: Future[bytes],
        *,
        wrapped: bool,
    ) -> None:
        try:
            reply_bytes = future.result()
        except BuildLost as e:
            self.logger.error(
                f"Build for {request.g_node_alias} slot {request.start_unix_s} lost: {e}"
            )
            return
        except BuildRefused as e:
            self.logger.error(
                f"Build for {request.g_node_alias} slot {request.start_unix_s} refused:\n{e}"
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
        self.reply(request, reply_bytes, age_s, wrapped=wrapped)

    def reply(
        self, request: FloParams, reply_bytes: bytes, age_s: float, *, wrapped: bool
    ) -> None:
        """The reply, in the kind the request came: direct over the edge to
        the bidder, or wrapped across the MQTT seam."""
        payload = json.loads(reply_bytes)
        if wrapped:
            self.send(
                envelope=self.wrapped_envelope(
                    type_name=REPLY_TYPE, to_class=TransportClass.LeafTransactiveNode
                ),
                body=wrap_bytes(
                    src=self.alias,
                    dst=request.g_node_alias,
                    inner_type_name=REPLY_TYPE,
                    inner_payload_dict=payload,
                ),
            )
        else:
            self.send(
                envelope=self.direct_envelope(
                    type_name=REPLY_TYPE,
                    to_class=TransportClass.LeafTransactiveNode,
                    to_alias=request.g_node_alias,
                ),
                body=reply_bytes,
            )
        self.answered += 1
        self.logger.info(
            f"Answered {request.g_node_alias} slot {request.start_unix_s} in {age_s:.1f} s: "
            f"{len(payload['PqPairs'])} pairs from {payload['FloName']}"
        )
