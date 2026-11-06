"""FloActor: the service's broker face.

It hears `flo.params` two ways and answers each in kind. A gwbase LTN
sends it direct over the LeafTransactiveNode-to-Flo edge and gets the
`flo.recommendation` direct over the edge back. An LTN still inside
gridworks-scada is an MQTT client: its request arrives wrapped on
`amq.topic` (`gw.<ltn>.to.flo.flo-params`) and its reply goes back
wrapped (`gw.<flo>.to.ltn.flo-recommendation`). The `amq.topic` binding
goes once the LTN has fully moved out of gridworks-scada.

The request's bytes go to the build pool, and when the build returns the
answer is published: the `flo.recommendation`, or a `flo.refusal` naming
why there is none (no optimizer for the tuning, no model for the plant,
a build that raised, a worker lost under it, or a recommendation that
came later than the deadline, since the LTN has bid without it by
then). The LTN pairs an answer with its request by BidderAlias and
StartUnixS, so no correlation id travels. Nothing is kept between
requests.

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
from gwflo.flos import refusal
from gwflo.pool import BuildLost, BuildPool, BuildRefused
from gwflo.sema.enums import FloRefusalReason
from gwflo.sema.types import FloParams, FloRecommendation, FloRefusal

LOGGER = logging.getLogger(__name__)

REQUEST_TYPE = FloParams.type_name_value()
REPLY_TYPE = FloRecommendation.type_name_value()
REFUSAL_TYPE = FloRefusal.type_name_value()


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
        self.refused = 0

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
            answer_bytes = future.result()
        except BuildLost as e:
            self.refuse(request, FloRefusalReason.BuildLost, str(e), wrapped=wrapped)
            return
        except BuildRefused as e:
            self.logger.error(
                f"Build for {request.g_node_alias} slot {request.start_unix_s} raised:\n{e}"
            )
            self.refuse(
                request,
                FloRefusalReason.BuildFailed,
                last_line(str(e)),
                wrapped=wrapped,
            )
            return
        payload = json.loads(answer_bytes)
        age_s = time.time() - received_s
        if payload["TypeName"] == REFUSAL_TYPE:
            self.answer(request, REFUSAL_TYPE, answer_bytes, wrapped=wrapped)
            self.refused += 1
            self.logger.warning(
                f"Refused {request.g_node_alias} slot {request.start_unix_s}: "
                f"{payload['Reason']}, {payload.get('Detail', '')}"
            )
            return
        if age_s > self.reply_deadline_s:
            self.refuse(
                request,
                FloRefusalReason.PastDeadline,
                f"built in {age_s:.0f} s, deadline {self.reply_deadline_s:.0f} s",
                wrapped=wrapped,
            )
            return
        self.answer(request, REPLY_TYPE, answer_bytes, wrapped=wrapped)
        self.answered += 1
        self.logger.info(
            f"Answered {request.g_node_alias} slot {request.start_unix_s} in {age_s:.1f} s: "
            f"{len(payload['PqPairs'])} pairs from {payload['FloName']}"
        )

    def refuse(
        self,
        request: FloParams,
        reason: FloRefusalReason,
        detail: str,
        *,
        wrapped: bool,
    ) -> None:
        self.answer(
            request,
            REFUSAL_TYPE,
            refusal(request, reason, detail).to_bytes(),
            wrapped=wrapped,
        )
        self.refused += 1
        self.logger.warning(
            f"Refused {request.g_node_alias} slot {request.start_unix_s}: {reason.value}, {detail}"
        )

    def answer(
        self, request: FloParams, type_name: str, answer_bytes: bytes, *, wrapped: bool
    ) -> None:
        """The answer, in the kind the request came: direct over the edge to
        the bidder, or wrapped across the MQTT seam."""
        if wrapped:
            self.send(
                envelope=self.wrapped_envelope(
                    type_name=type_name, to_class=TransportClass.LeafTransactiveNode
                ),
                body=wrap_bytes(
                    src=self.alias,
                    dst=request.g_node_alias,
                    inner_type_name=type_name,
                    inner_payload_dict=json.loads(answer_bytes),
                ),
            )
        else:
            self.send(
                envelope=self.direct_envelope(
                    type_name=type_name,
                    to_class=TransportClass.LeafTransactiveNode,
                    to_alias=request.g_node_alias,
                ),
                body=answer_bytes,
            )


def last_line(text: str) -> str:
    """The exception line of a worker's stderr: the end of its traceback."""
    lines = [line for line in text.splitlines() if line.strip()]
    return lines[-1] if lines else "no output"
