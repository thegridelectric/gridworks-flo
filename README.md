# gridworks-flo

The FLO service. A Forward Looking Optimizer takes one `flo.params`
request for a house and a market slot and answers with one
`flo.recommendation`: a bid complete over prices, one price-quantity pair
per quantity change from the market ceiling down, and beside each pair the
plan that pair's choice leads to. The FLO never sees the price that
clears; the bidder picks the cleared pair off the recommendation it holds.

This service is the broker face for every FLO a GridWorks LTN can name:
one actor (`gwflo rabbit`) hears requests, dispatches on the request's
`Tuning` TypeName, runs the build in a bounded pool of worker processes
and publishes the reply. Nothing is kept between requests.

## The FLOs

| Tuning word | FLO | where |
|---|---|---|
| `flo.tuning.alice` | buys nothing: one pair at the ceiling, zero quantity, a zero-power path | `src/gwflo/flos.py` |
| `flo.tuning.bruce` | the graph optimizer over a heat pump and a stratified water store | the `bruce` package (`optimal-flexibility`), through `bruce.service.recommend` |

The two repos share the vocabulary and no Python types: the request
crosses to bruce as bytes and the reply comes back as bytes.

## The seam

The LTN speaks MQTT; this actor speaks AMQP. They meet on the broker's
`amq.topic`:

- request: the LTN publishes a wrapped `flo.params` on
  `gw/<ltn-alias-hyphenated>/to/flo/flo-params`; the actor's queue is
  bound to `gw.*.to.flo.flo-params`.
- reply: the actor publishes a wrapped `flo.recommendation` on
  `gw.<flo-alias-hyphenated>.to.ltn.flo-recommendation`, which the LTN
  hears as `gw/<flo-alias-hyphenated>/to/ltn/flo-recommendation`.

No correlation id: the reply names the bidder (`BidderAlias`) and the
slot (every path's `StartUnixS`), and the LTN keeps only the reply for
the slot it is bidding. A reply older than `GWFLO_REPLY_DEADLINE_S` is
dropped rather than sent.

## Workers and memory

Each build runs in a process that exits when done, so the OS reclaims the
graph. `GWFLO_WORKERS` builds run at once and the rest queue. The unit's
`MemoryMax` is sized for that many builds at the largest plant shape; a
build past it is killed, the pool replaces the worker, and the request
is logged as lost. bruce's transition matrices are never built in the
request path: a request whose plant shape has no cached matrix is refused
and logged.

## Run

```shell
uv sync --all-groups --all-extras   # --extra bruce installs the sibling ../optimal-flexibility editable
cp template.env .env                # and fill in
uv run gwflo rabbit
```

bruce is the sibling checkout `../optimal-flexibility`
(`thdfw/optimal-flexibility`), installed editable, so a bruce update is a
pull of the sibling. CI clones the sibling beside the checkout and runs
without the extra.

`./ci.sh` is the gate: ruff, pyright, pytest. The broker tests need the
gwbase dev broker (`gw-dev-rabbit`: AMQP on 5672, MQTT on 1885) and
self-skip without it. The vendored sema snapshot under `src/gwflo/sema`
is generated: edit `src/gwflo/sema_seed_request.yaml` and run
`scripts/regen_sema_snapshot.sh`.

## Deploy

One login `flo`, one unit `service/flo-rabbit.service`, one FIS
principal. The checkout on the box is a pushed `main` SHA.
