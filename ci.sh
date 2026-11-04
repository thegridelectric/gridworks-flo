#!/usr/bin/env bash
#
# The gate for this repo: the GitHub workflow runs this script and nothing
# else, so a local pass is a CI pass. Run it before committing.
#
# The broker tests need the gwbase dev broker (gw-dev-rabbit, started from
# gridworks-base with ./arm.sh or ./x86.sh); without one they self-skip.
# bruce is the editable sibling checkout ../optimal-flexibility.
#
# Usage: ./ci.sh
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

step() { printf '\n=== %s ===\n' "$1"; shift; "$@"; }

step "uv sync (locked)" uv sync --all-groups --all-extras --locked
step "ruff check" uv run ruff check --no-fix .
step "ruff format --check" uv run ruff format --check .
step "pyright" uv run pyright

if ! nc -z localhost 5672 2>/dev/null; then
  echo "NOTE: no broker on localhost:5672 — broker tests will self-skip."
fi
step "tests" uv run pytest -q

printf '\nAll checks passed.\n'
