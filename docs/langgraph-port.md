# LangGraph + AgentCore port of the supply-chain demo

Goal: the same workflow as `cmd/supplychain-agent` (query low stock, group by vendor,
send one purchase order per vendor, exactly once) expressed as a LangGraph graph, so the
checkpoint/resume story can be told in the framework most teams actually use.

## What stays the same
- The three steps: `query_low_stock` -> `decide_orders` (LLM) -> `send_purchase_order` per vendor.
- Postgres is the source of truth for run state.
- Crash mid-run, resume, same result, no second purchase order.

## What changes
| bulkhead (Go) | LangGraph port (Python) |
|---|---|
| control plane answers steps from `steps` table | `PostgresSaver` checkpointer answers nodes from `checkpoints` |
| agent blocks on stdin for each step | each node is a Python function on `State` |
| leased queue (SKIP LOCKED) redelivers the run | AgentCore Runtime re-invokes with the same `thread_id` |
| outbox idempotency key per PO | same: `po_key = hash(run_id, vendor)` checked in `purchase_orders` before send |
| llm step replays stored completion | checkpointer replays `decide_orders` output; model not re-called |

## Phases
1. `agents/langgraph/graph.py` — StateGraph, 3 nodes, `PostgresSaver`, deterministic decide by
   default, `USE_LLM=1` routes decide through Claude via Bedrock `converse`. Verify: pytest kills
   the process after `decide_orders`, re-invokes with the same thread_id, asserts one PO row.
2. Local entrypoint `agents/langgraph/main.py` using `bedrock-agentcore` SDK's `BedrockAgentCoreApp`
   so the same file runs locally and on Runtime. Verify: `agentcore dev` loopback invoke.
3. Deploy to AgentCore Runtime (us-west-2) with Postgres on the existing sandbox RDS or a Neon
   free tier. Verify: invoke, kill the session mid-run (`agentcore stop-session`), re-invoke,
   one PO. Capture CloudWatch spans for the README.
4. Cost tracking: log input/output tokens + Runtime vCPU-seconds per run to a `run_costs` table;
   expose in the README as "cost per run".
5. README section "what broke and how I fixed it" written from the actual failures in 1-3.

## Cost guard
Runtime v2: $0.1276/vCPU-hr, $0.0169/GB-hr, 1-second minimum, idle memory reclaimed after
120s. A run is a few seconds of CPU, so dozens of runs are cents. Set an AWS Budget alert at
$5 before phase 3, delete the Runtime agent when done. Observability is CloudWatch pricing;
keep log retention at 1 day.

## Out of scope
Memory, Gateway, Identity. Mention them in the README as the parts not exercised.

## What broke on the way to Runtime (2026-10-17, in order)

1. **CodeZip rejected: "OpenTelemetry instrumentation executable not found."** Runtime turns
   observability on by default and expects `aws-opentelemetry-distro` inside the zip. Added it to
   `pyproject.toml`; the CLI packages from `uv.lock`, so `uv lock` first.
2. **Bedrock `ResourceNotFoundException: Model use case details have not been submitted for this
   account.`** The Runtime's execution role has `bedrock:InvokeModel`; the block is the account-level
   Anthropic use-case agreement (`get-foundation-model-availability` shows
   `agreementAvailability: NOT_AVAILABLE`). One-time console form. Ran the crash test with the
   model step off until it clears.
3. **`agentcore invoke --prompt-file` wraps the file as `{"prompt": "<text>"}`.** JSON flags never
   reached the entrypoint; `thread_id` silently fell back to the session id. `app.py` now unwraps a
   JSON string in `prompt`; the demo drives `aws bedrock-agentcore invoke-agent-runtime` directly.
4. **Reseed read `schema.sql`/`seed.sql` from the Go tree**, which is not in the zip. Packaged a copy
   under `bulkhead_langgraph/sql/` with a test that it is byte-identical to the Go files.
5. **A crashed session does not come back.** Re-invoking the session whose process died returns
   502 every time: a Runtime session is bound to its microVM. The resume has to arrive on a new
   session id with the same `thread_id`. This is the real difference from the Go runtime's leased
   queue, where redelivery is automatic: on AgentCore the caller owns the retry, and the LangGraph
   checkpointer plus the idempotency key are what make that retry safe.

Crash-and-resume verified on Runtime `bulkheadlanggraph_supplychain-aU9hU58oN9` (us-west-2):
after the crash Neon shows 1 purchase order, 0 Slack rows; after the resume on a fresh session,
"duplicate, not re-sent", 1 purchase order, 1 Slack row.
