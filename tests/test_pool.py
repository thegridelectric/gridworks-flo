"""The build pool: one subprocess per build, the request on stdin and
the reply on stdout, and the three ways a build ends."""

from __future__ import annotations

import subprocess
import sys

import pytest

from gwflo.pool import BuildLost, BuildPool, BuildRefused
from gwflo.sema.types import FloRecommendation
from tests.conftest import alice_request


def test_a_build_answers_over_the_pipes() -> None:
    pool = BuildPool(1, timeout_s=60)
    try:
        reply = FloRecommendation.from_bytes(
            pool.submit(alice_request().to_bytes()).result()
        )
    finally:
        pool.shutdown()
    assert reply.flo_name == "alice"
    assert reply.bidder_alias == alice_request().g_node_alias


def test_a_build_that_raises_is_refused_with_its_stderr() -> None:
    raising = (sys.executable, "-c", "raise ValueError('bad tank')")
    pool = BuildPool(1, timeout_s=60, argv=raising)
    try:
        with pytest.raises(BuildRefused, match="ValueError: bad tank"):
            pool.submit(b"{}").result()
    finally:
        pool.shutdown()


def test_a_killed_worker_is_a_lost_build() -> None:
    killed = (
        sys.executable,
        "-c",
        "import os, signal; os.kill(os.getpid(), signal.SIGKILL)",
    )
    pool = BuildPool(1, timeout_s=60, argv=killed)
    try:
        with pytest.raises(BuildLost, match="signal 9"):
            pool.submit(b"{}").result()
    finally:
        pool.shutdown()


def test_a_build_past_the_timeout_is_lost() -> None:
    hung = (sys.executable, "-c", "import time; time.sleep(30)")
    pool = BuildPool(1, argv=hung, timeout_s=0.5)
    try:
        with pytest.raises(BuildLost, match="timed out"):
            pool.submit(b"{}").result()
    finally:
        pool.shutdown()


def test_only_the_reply_reaches_stdout() -> None:
    """The worker claims the stdout pipe for the reply alone: a write to
    file descriptor 1, Python or C, lands on stderr."""
    script = (
        "import os; from gwflo.build import claim_stdout; fd = claim_stdout(); "
        "os.write(1, b'noise'); print('more'); write_reply(fd, b'reply')"
    ).replace("write_reply", "__import__('gwflo.build').build.write_reply")
    done = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, check=True
    )
    assert done.stdout == b"reply"
    assert b"noise" in done.stderr
    assert b"more" in done.stderr
