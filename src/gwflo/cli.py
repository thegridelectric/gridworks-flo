"""gwflo console entry point: `gwflo rabbit` runs the FLO actor, the one
systemd unit (`flo-rabbit.service`)."""

from __future__ import annotations

import argparse
import logging
import sys
import time

import dotenv

from gwflo.config import GwfloSettings
from gwflo.flo_actor import FloActor
from gwflo.pool import BuildPool


def run_rabbit(settings: GwfloSettings) -> None:
    logging.basicConfig(
        level=settings.log_level.upper(),
        stream=sys.stdout,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    actor = FloActor(
        settings=settings,
        pool=BuildPool(settings.workers, timeout_s=settings.reply_deadline_s),
    )
    actor.start()
    try:
        while actor.main_loop_running:
            time.sleep(1)
    except KeyboardInterrupt:
        actor.stop()


def main(argv: list[str] | None = None) -> None:
    dotenv.load_dotenv(dotenv.find_dotenv())  # populate env BEFORE Settings()
    parser = argparse.ArgumentParser(prog="gwflo")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("rabbit", help="run the FLO actor")
    args = parser.parse_args(argv)
    if args.command == "rabbit":
        run_rabbit(GwfloSettings.load())
