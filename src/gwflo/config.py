"""Settings for gridworks-flo."""

from gwbase.config import GNodeSettings
from gwbase.transport_format import LeftRightDot
from pydantic_settings import SettingsConfigDict


class GwfloSettings(GNodeSettings):
    """Reads from env (GWFLO_*) and/or a `.env` file at the repo root.

    Inherits `rabbit: RabbitBrokerClient`, `g_node_path`, `log_level` and
    the rest of `ServiceSettings` from `GNodeSettings`. The FLO is a
    GNode: `service_alias` MUST match the Alias in its `g.node.gt.json`,
    which `GridworksActor` enforces at boot. The universe-level FLO is
    `<universe>.flo` under the universe root.
    """

    service_alias: LeftRightDot = "d1.flo"
    service_name: str = "flo"  # XDG path segment for logs/state

    # The Orchestrator tier's control plane: whose heartbeats to answer and
    # whose simulated time to follow. No defaults: each universe names its
    # own.
    super_alias: LeftRightDot
    time_coordinator_alias: LeftRightDot

    # The worker pool: builds that run at once (the box is sized for this
    # many all-tanks peaks) and the age past which a reply is dropped
    # instead of sent, since the LTN has bid without it by then.
    workers: int
    reply_deadline_s: float

    model_config = SettingsConfigDict(
        env_prefix="GWFLO_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    @classmethod
    def load(cls) -> "GwfloSettings":
        """Settings from env and `.env`. The fields with no default are what
        pyright reads as missing constructor arguments; the settings
        sources supply them, so this is the one place that call is made."""
        return cls()  # pyright: ignore[reportCallIssue]
