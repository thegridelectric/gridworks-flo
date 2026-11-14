# gridworks-flo

The FLO service works out what a house should bid for its electricity.
A GridWorks house buys the electricity for its heat pump hour by hour in
a market, through a bidder that acts for it. Before each hour, the
bidder sends the FLO what it knows about the house: its hot water store
and how full it is, its heat pump, and the weather and price forecasts
for the hours ahead. The FLO answers with a bid curve: how much
electricity the house would buy at each price, from the market's highest
price down, and beside each step of the curve, the plan for the heat
pump and the store over the forecast hours that buying that much leads
to. Each answer is worked out from its request alone, and the service
keeps nothing between requests. FLO stands for Forward Looking
Optimizer.

## What it speaks

One request word, `flo.params`, and one of two answers: `flo.recommendation`
or `flo.refusal`. The recommendation is complete over prices: one
price-quantity pair per quantity change from the request's price ceiling
down, and one optimal path per pair at the same index. It names no market
slot and no sign convention.

The request's `Tuning` names the FLO, and the service dispatches on the
`Tuning` TypeName and on nothing else.

| Tuning word | FLO |
|---|---|
| `flo.tuning.alice` | buys nothing: one pair at the ceiling, zero quantity, a zero-power path |
| `flo.tuning.bruce` | the graph optimizer over a heat pump and a stratified water store, in `thdfw/optimal-flexibility` |

The service and bruce share the vocabulary and nothing else: the request
crosses to bruce as bytes and the reply comes back as bytes.

A request the service cannot serve is answered with `flo.refusal` in
place of the recommendation, with a `Reason`: no optimizer for the
tuning, no cached matrix for the plant shape, a build that raised, a
build lost under it, or a recommendation built later than
`GWFLO_REPLY_DEADLINE_S`, the longest a bidder waits for an answer.

There is no correlation id. The answer names the bidder (`BidderAlias`)
and the slot (the first path's `StartUnixS`), and those two pair a reply
with its request.

## The two ways in

Every request is answered the way it came.

- Direct: a gwbase LTN sends `flo.params` over the
  LeafTransactiveNode-to-Flo edge of the class fabric, addressed to this
  FLO by alias, and the `flo.recommendation` goes back direct over the
  edge to the bidder.
- Wrapped, across the MQTT seam: an LTN that speaks MQTT publishes a
  wrapped `flo.params` on `gw/<ltn-alias-hyphenated>/to/flo/flo-params`.
  The service's queue is bound to `gw.*.to.flo.flo-params` on
  `amq.topic`, and the reply goes back wrapped on
  `gw.<flo-alias-hyphenated>.to.ltn.flo-recommendation`.

## Builds and memory

Each build runs in a process of its own, the request in and the reply
out over its pipes, and exits when it is done, so its memory goes back to
the operating system. `GWFLO_WORKERS` builds run at once and the rest
queue. The service's memory cap is sized for that many builds at the
largest plant shape. A build past the cap, or still running at
`GWFLO_REPLY_DEADLINE_S`, is killed and its request refused.

bruce's transition matrices are generated ahead of time, one per plant
shape, and a request only ever reads one. A request whose plant shape has
no cached matrix is refused.

## Deploy

One login `flo`, one unit `service/flo-rabbit.service`, one GNode: the
registry row `<universe>.flo`, its `g.node.gt.json` on the box and the
FIS principal whose cert CN is that GNodeId. The checkout on the box is
a pushed `main` SHA.

## Run

```shell
uv sync --all-groups --all-extras   # --extra bruce installs the sibling ../optimal-flexibility editable
cp template.env .env                # and fill in
uv run gwflo rabbit
```

bruce is the sibling checkout `../optimal-flexibility`
(`thdfw/optimal-flexibility`), installed editable, so a bruce update is a
pull of the sibling. CI clones the sibling beside the checkout and runs
without the extra. A build is `python -m gwflo.build`.

`./ci.sh` is the gate: ruff, pyright, pytest. The broker tests need the
gwbase dev broker (`gw-dev-rabbit`: AMQP on 5672, MQTT on 1885) and
self-skip without it. The vendored sema snapshot under `src/gwflo/sema`
is generated: edit `src/gwflo/sema_seed_request.yaml` and run
`scripts/regen_sema_snapshot.sh`.
