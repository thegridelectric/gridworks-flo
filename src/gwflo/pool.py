"""A bounded pool of build workers.

Each build runs in a process of its own that exits when the build is
done, so the OS reclaims the graph. `workers` processes run at once and
further requests queue. A worker killed under it (the unit's MemoryMax,
a crash) breaks the executor; the pool replaces it and the request it
was running is lost, which the caller reports.
"""

from __future__ import annotations

import logging
import multiprocessing
from concurrent.futures import Future, ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from threading import Lock

from gwflo.flos import run

LOGGER = logging.getLogger(__name__)


class BuildPool:
    def __init__(self, workers: int) -> None:
        self.workers = workers
        self.lock = Lock()
        self.executor = self.new_executor()

    def new_executor(self) -> ProcessPoolExecutor:
        return ProcessPoolExecutor(
            max_workers=self.workers,
            mp_context=multiprocessing.get_context("spawn"),
            max_tasks_per_child=1,
        )

    def submit(self, request_bytes: bytes) -> Future[bytes]:
        with self.lock:
            try:
                return self.executor.submit(run, request_bytes)
            except BrokenProcessPool:
                LOGGER.error("Build pool broken; replacing it")
                self.executor = self.new_executor()
                return self.executor.submit(run, request_bytes)

    def shutdown(self) -> None:
        with self.lock:
            # wait=True: with max_tasks_per_child the executor's management
            # thread respawns a worker after each build, and a non-waiting
            # shutdown races it. Queued builds are cancelled; a build in
            # flight finishes first (the unit's TimeoutStopSec allows it).
            self.executor.shutdown(wait=True, cancel_futures=True)
