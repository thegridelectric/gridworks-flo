"""A bounded pool of build workers.

Each build is a subprocess of its own, fed the request on stdin and read
on stdout, that exits when the build is done so the OS reclaims the
graph. `workers` builds run at once and further requests queue, bounded
by a thread pool in this process. The worker shares no kernel object
with the service beyond its pipes, so nothing outside the service (a
login session ending, a cleanup of /dev/shm) can break the pool. A
worker that exits with an error (a refused request, a build that
raised) raises `BuildRefused` with its stderr; one killed under it (the
unit's MemoryMax, a crash) or still running at `timeout_s` is killed and
raises `BuildLost`.
"""

from __future__ import annotations

import logging
import subprocess
import sys
from collections.abc import Sequence
from concurrent.futures import Future, ThreadPoolExecutor

LOGGER = logging.getLogger(__name__)

WORKER_ARGV: Sequence[str] = (sys.executable, "-m", "gwflo.build")


class BuildRefused(Exception):
    """The worker exited with an error: the request could not be served."""


class BuildLost(Exception):
    """The worker was killed before it answered."""


def build(request_bytes: bytes, argv: Sequence[str], timeout_s: float) -> bytes:
    try:
        done = subprocess.run(
            argv,
            input=request_bytes,
            capture_output=True,
            check=False,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired as e:
        raise BuildLost(
            f"worker timed out after {timeout_s:.0f} s and was killed"
        ) from e
    if done.returncode < 0:
        raise BuildLost(f"worker killed by signal {-done.returncode}")
    if done.returncode != 0:
        raise BuildRefused(done.stderr.decode(errors="replace").strip())
    return done.stdout


class BuildPool:
    def __init__(
        self, workers: int, timeout_s: float, argv: Sequence[str] = WORKER_ARGV
    ) -> None:
        self.workers = workers
        self.timeout_s = timeout_s
        self.argv = argv
        self.executor = ThreadPoolExecutor(max_workers=workers)

    def submit(self, request_bytes: bytes) -> Future[bytes]:
        return self.executor.submit(build, request_bytes, self.argv, self.timeout_s)

    def shutdown(self) -> None:
        # Queued builds are cancelled; a build in flight finishes first
        # (the unit's TimeoutStopSec allows it).
        self.executor.shutdown(wait=True, cancel_futures=True)
