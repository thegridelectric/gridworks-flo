"""The build worker: one process per build.

Reads a `flo.params` as bytes on stdin, runs the FLO its Tuning names,
writes the `flo.recommendation` bytes to stdout and exits, so the OS
reclaims the graph. The stdout pipe is claimed for the reply alone:
anything the build writes to file descriptor 1, from Python or from a C
library, goes to stderr, which the parent logs only when the build fails.
"""

from __future__ import annotations

import os
import sys

from gwflo.flos import run


def claim_stdout() -> int:
    """Take the stdout pipe for the reply: returns a private descriptor on
    it and points descriptor 1 at stderr for everything else."""
    reply_fd = os.dup(1)
    os.dup2(2, 1)
    return reply_fd


def write_reply(reply_fd: int, reply_bytes: bytes) -> None:
    view = memoryview(reply_bytes)
    while view:
        view = view[os.write(reply_fd, view) :]
    os.close(reply_fd)


def main() -> None:
    reply_fd = claim_stdout()
    request_bytes = sys.stdin.buffer.read()
    write_reply(reply_fd, run(request_bytes))


if __name__ == "__main__":
    main()
