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

## The acceptance test

Milestone 1 has exactly one definition of done, and it is executable:

```sh
go test -tags acceptance ./test/acceptance/... -v
```

Start a run, let it get past its first tool call, `kill -9` the control plane, restart it. The run
resumes from its last checkpoint and produces an identical result — and every step is executed
exactly once across the interruption.

## License

MIT
