from typing import Literal
from gwflo.sema.base import SemaType
from gwflo.sema.enums import FloRefusalReason
from gwflo.sema.property_format import LeftRightDot
from gwflo.sema.property_format import NonEmptyString
from gwflo.sema.property_format import UTCMilliseconds
from gwflo.sema.property_format import UTCSeconds


class FloRefusal(SemaType):
    """Sema: https://schemas.electricity.works/types/flo.refusal/000"""

    bidder_alias: LeftRightDot
    start_unix_s: UTCSeconds
    reason: FloRefusalReason
    detail: NonEmptyString | None = None
    created_ms: UTCMilliseconds
    type_name: Literal["flo.refusal"] = "flo.refusal"
    version: Literal["000"] = "000"
