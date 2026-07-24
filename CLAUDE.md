# bulkhead — working notes

An execution harness for agent-generated code. Read `README.md` for what it is, `ROADMAP.md` for
the milestone list and each one's verify criterion, and `docs/design.md` for why the agent can't
execute its own steps.

## Decisions already made — do not re-litigate

- **Go for the control plane, Python for the agent and evals.** The Go concurrency code is what a
  reviewer opens first and what differentiates this from every other agent repo. If scope has to
  be cut, cut a sandbox tier or a workload — never the control plane.
- **Code execution, not document extraction,** as the primary workload. The sandbox has to be
  load-bearing; running trusted code over untrusted data would make the isolation benchmark
  decoration.
- **Postgres for durable checkpoints.** Redis is hot state and arrives with Milestone 3, not
  before.
- **The agent is a pure step generator.** It asks for a step and blocks; the control plane decides
  whether that step executes or is answered from a checkpoint. This is the load-bearing decision —
  see `docs/design.md`.
- **Public repo from commit 1.** Nothing career-related, personal, or job-search-related goes in
  this repository.

## The two things that make this worth building

Not the repo — the outputs:

1. A comparative isolation benchmark across container / gVisor / Firecracker / WASM on identical
   agent workloads. Nobody has published a clean version of this for agent workloads.
2. A failure write-up: where it broke under load, the fix, and where it breaks next at 10×.

Judge proposed additions against those. If something would make this a better infrastructure
tutorial but not a better answer to "what happens at 500 concurrent runs," it's out of scope.

## Conventions

- The Go↔Python boundary is newline-delimited JSON over stdin/stdout, defined in
  `internal/protocol`. Keep it dumb — no shared state across the line. That boundary is what later
  lets the agent run in a different sandbox tier or on a different node.
- **Write intent before doing work.** A step row is written `pending` before the step executes and
  updated to `completed` after, never the reverse. Everything about resume depends on this.
- Only `completed` steps are replayed. A `pending` step is ambiguous and is executed again — the
  at-least-once window, closed in Milestone 6 with idempotency keys.
- New sandbox tiers and new LLM providers go behind the existing interface with no caller changes.
  A tier or provider that requires touching the control plane is a design failure, not a feature.

## Cost discipline

Only **resolve rate** depends on which model the agent uses. Throughput, latency percentiles,
cold-start, density, and the failure taxonomy are harness properties — identical whether the
patches are good or garbage. So: develop against the fixture agent (free) or the cheapest
available provider, and spend on a capable model only for the one number that warrants it.

## Running things

```sh
docker compose up -d                                   # Postgres on 5432
go build ./...
go test -tags acceptance ./test/acceptance/... -v      # the Milestone 1 definition of done
```

The acceptance test builds both binaries, runs the control plane as a child process, kills it
mid-run with an uncatchable signal, and restarts it. It uses a deterministic Go fixture agent, so
it needs no API key and costs nothing to run. Keep it that way.
