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
