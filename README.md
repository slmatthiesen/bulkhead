# bulkhead

An open-source, self-hostable **execution harness for agent-generated code** — sandboxed,
checkpointed, queue-dispatched, Kubernetes-scaled.

Named for the resilience pattern: compartmentalize so one flooded section doesn't sink the ship.
That is what sandbox isolation and failure containment do here.

The point is not the agent. The point is the **runtime the agent executes in**, and the numbers
that come out of it.

## Status

Milestone 1 of 8, in progress. See [ROADMAP.md](ROADMAP.md).

## Why

Agent frameworks are abundant; the runtime underneath them is mostly hidden inside vendors. This
repo is the runtime, in the open, with published measurements:

1. **A comparative isolation benchmark** — an identical agent workload run across plain containers,
   gVisor, Firecracker microVMs, and WASM/WASI. Cold start, warm-pool checkout latency, per-sandbox
   memory, max density per node, syscall overhead, and what breaks in each tier.
2. **A scale run with free ground truth** — SWE-bench Verified. The agent reads an issue and writes
   a patch; the harness runs the repository's own test suite in a sandbox. Tests pass or they
   don't, so there is nothing to argue about. Published: throughput, p50/p95/p99, cost per 100
   instances, resolve rate, failure taxonomy.

## Architecture

| Component | Choice |
|---|---|
| Control plane / scheduler | Go — goroutines, `context`, gRPC API |
| Work dispatch | Redis Streams, migrating to Kafka partitions |
| Trajectory event log | Kafka (KRaft mode) — append every step, replay from offset |
| Hot state | Redis — sandbox pool leases, rate limiters, idempotency keys |
| Durable checkpoints | Postgres — run state, resumable trajectories |
| Sandbox pool | Warm pool with checkout/return, four tiers behind one interface |
| Orchestration | k3s, sandbox pool as a scaled workload |
| Autoscaling | KEDA on queue depth / consumer lag |
| Observability | Prometheus + Grafana + OpenTelemetry |
| Agent + eval layer | Python, MCP tools |

Go for the control plane, Python for agents and evals. The Go surface is deliberately small —
scheduler, sandbox pool with leases, queue consumer, API — because that is where the concurrency
lives. The two halves talk over one dumb boundary: the control plane spawns the agent as a
subprocess and reads newline-delimited JSON events from its stdout. No shared state across the
line, which is what later lets the agent run in a different sandbox tier or on a different node.

## Requirements

- Go 1.24+
- Docker with the Linux engine (sandbox tier 1, and Postgres for local development)
- Python 3.12+ and [uv](https://docs.astral.sh/uv/) for the agent

## Quick start

```sh
docker compose up -d          # Postgres on 5432
go build ./...
go run ./cmd/bulkheadd        # control plane on 127.0.0.1:8080
```

## Demo Video

> **[▶️ Watch the 5-Minute Bulkhead Crash Recovery & Idempotency Demo](https://share.descript.com/view/8xbsqxxxybX)**  
> Demonstrates a hard crash (`kill -9`) mid-transaction, automatic lease expiration via Postgres `FOR UPDATE SKIP LOCKED`, checkpoint replay, and idempotency key deduplication to prevent double-spending purchase orders.

## Watching a run resume

The control plane serves a trajectory viewer at <http://127.0.0.1:8080>. It tails
`GET /runs/{id}/events` — server-sent events read straight off the append-only trajectory in
Postgres — and draws one node per step.

```sh
go build -o bin/ ./cmd/...
BULKHEAD_AGENT_CMD=./bin/fixture-agent BULKHEAD_FIXTURE_STEP_DELAY=700ms ./bin/bulkheadd
```

Open the page, press **Start run**, then `kill -9` the control plane while steps are still
executing and start it again. The browser's `EventSource` reconnects with the last event id it
saw, so the stream picks up exactly where it stopped — and what arrives is a second `run_started`,
then the already-checkpointed steps coming back as **replayed** rather than executing again.

The 700ms step delay only exists to make the window wide enough to kill by hand.

## The acceptance test

Milestone 1 has exactly one definition of done, and it is executable:

```sh
go test -tags acceptance ./test/acceptance/... -v
```

Start a run, let it get past its first tool call, `kill -9` the control plane, restart it. The run
resumes from its last checkpoint and produces an identical result, and every step whose output was
already durable is replayed rather than re-executed.

At most one step repeats: the one in flight when the process died has a durable record that it
started and none that it finished, so the control plane cannot know whether its side effect
happened and has to assume it did not. That is the at-least-once window, and closing it with
idempotency keys is Milestone 6. The test asserts the bound rather than pretending it isn't there.

## License

MIT
