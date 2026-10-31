# The supply-chain demo on LangGraph + Amazon Bedrock AgentCore

The same workflow as [`cmd/supplychain-agent`](../../cmd/supplychain-agent/main.go), rebuilt as a
LangGraph graph and deployed to Bedrock AgentCore Runtime. Same database, same tables, same
idempotency keys, same test: kill the process after the purchase order is sent and before its
checkpoint is written, resume, and the vendor still has exactly one order.

The Go runtime is the thing I built to understand the problem. This is the same problem solved with
the parts most teams actually use, so the two can be compared honestly.

## What maps to what

| Go runtime | LangGraph + AgentCore |
|---|---|
| Agent is a subprocess that asks for steps over NDJSON | Five Python functions as graph nodes |
| Control plane replays completed steps from its `steps` table | `PostgresSaver` checkpointer replays completed nodes |
| Approval step polls the `approvals` table until a human replies | `interrupt()` stops the graph; the caller resumes with the reply |
| Leased Postgres queue redelivers a crashed run automatically | **Nothing redelivers.** The caller re-invokes with the same `thread_id` on a new session |
| Process supervised by `bulkhead-demo` | AgentCore Runtime microVM, one per session |
| Secrets in `.env` | Connection string in AgentCore Identity, fetched at startup |
| slog | CloudWatch logs and X-Ray traces via the OpenTelemetry distro |
| Optional model step via an OpenAI-compatible endpoint | Claude or Nova via the Bedrock Converse API through LangChain |

## Under the hood

```
 caller (aws bedrock-agentcore invoke-agent-runtime, or agentcore invoke)
   │  payload {"thread_id", "resume"?, "question"?, "reseed"?, "crash_after_send"?}
   ▼
 AgentCore Runtime ── one microVM per runtime-session-id ── execution role (IAM)
   │  app.py: BedrockAgentCoreApp entrypoint, unwraps {"prompt": "<json>"}
   ▼
 runner.run_thread(thread_id)
   │  1. store.database_url()  ── first call fetches the Neon URL from AgentCore Identity
   │  2. PostgresSaver.setup()  ── checkpoint tables in the same Neon database
   │  3. graph.invoke(payload, {"configurable": {"thread_id": thread_id}})
   ▼
 StateGraph                              Postgres (Neon)
   START → query_low_stock ─────────────▶ inventory, vendors      (read)
         → explain ─────── Bedrock Converse (Claude / Nova) ──▶ checkpoint stores the completion
         → approve ─────── interrupt()  ── graph stops; checkpoint says "waiting"
                           ▲ resume with Command(resume="yes") on any later invocation
         → send_po ───────────────────────▶ purchase_orders       (insert, unique idempotency_key)
                           ✖ crash_after_send: os._exit(137) here, before the checkpoint
         → notify ────────────────────────▶ slack_notifications   (insert, unique idempotency_key)
         → more vendors? → approve … else END
   │
   ▼
 costs.record() ──▶ run_costs (model, tokens, wall_ms, usd)    then the response
```

What each layer owns:

- **Runtime** owns the process and its session. A session whose process dies is gone (502 on
  re-invoke); it does not redeliver. The caller re-invokes with a new session id and the same
  `thread_id`.
- **Checkpointer** owns progress. Every completed node is written before the next runs, so a resumed
  thread skips what finished and re-runs the node that was in flight. The model's completion is
  part of that state, so a resume never calls the model again.
- **Idempotency keys** own the side effects. `send_po` can be re-run blindly because the insert
  collides on `run_id:vendor:email` and is reported as a duplicate instead of sent twice.
- **Identity** owns the secret. The database URL is never in the zip, the config, or the repo.
- **CloudWatch** owns the trace. The OpenTelemetry distro in the zip is what Runtime requires.

## The run

```
1  invoke (session A)   {"thread_id": T, "reseed": true, "question": "What runs out first and when?"}
   -> awaiting_approval   model: "Vitamin C 1000mg runs out first in 2.0 days."   109 in / 19 out tokens

2  invoke (session A)   {"thread_id": T, "resume": "yes", "crash_after_send": true}
   -> 502. The process exited after the purchase_orders insert, before the checkpoint.
   Neon: approvals 1, purchase_orders 1, slack_notifications 0

3  invoke (session B)   {"thread_id": T}
   -> "purchase order 6 duplicate, not re-sent", "slack posted", status done
   Neon: approvals 1, purchase_orders 1, slack_notifications 1
   explanation replayed from the checkpoint: 0 tokens
```

Cost of that thread, from the `run_costs` table the runner writes:

| invocation | wall | tokens | model $ |
|---|---|---|---|
| start (model call) | 2.4 s | 109 / 19 | $0.000148 |
| crash | died before the row | | |
| resume | 0.4 s | 0 / 0 | $0 |

Runtime compute at the published v2 rate is $0.1276 per vCPU-hour, billed per second with a
1-second minimum, so the three invocations above are on the order of a tenth of a cent. The model is
called once per thread no matter how many times the run is resumed; that is what the checkpoint buys.

## What broke, in order

1. **Runtime refused the zip: "OpenTelemetry instrumentation executable not found."** Observability
   is on by default and the zip must carry `aws-opentelemetry-distro`. Add it, `uv lock`, redeploy.
2. **Bedrock: "Model use case details have not been submitted for this account."** The execution
   role had `bedrock:InvokeModel`; the block was the account-level Anthropic agreement. The console
   button never created it. `aws bedrock list-foundation-model-agreement-offers` plus
   `create-foundation-model-agreement` did. Ran on Nova Pro in the meantime, same code path.
3. **`agentcore invoke --prompt-file` wraps the file as `{"prompt": "<text>"}`.** The JSON flags
   never reached the entrypoint and `thread_id` silently fell back to the session id. The entrypoint
   now unwraps a JSON string in `prompt`; the demo calls `aws bedrock-agentcore invoke-agent-runtime`
   directly, which sends the payload verbatim.
4. **Reseed read `schema.sql` from the Go tree**, which is not in the zip. The SQL is packaged with
   a test that it is byte-identical to the Go files.
5. **A crashed session does not come back.** Re-invoking the session whose process died returns
   502 forever: a Runtime session is bound to its microVM. The resume has to arrive on a new session
   with the same `thread_id`. This is the real difference from the Go runtime, where the leased queue
   redelivers on its own. On AgentCore the caller owns the retry; the checkpointer and the
   idempotency key are what make that retry safe to issue blindly.

## Run it

```sh
cd agents/langgraph
uv venv && uv pip install -e ".[dev]"
docker compose -f ../../docker-compose.yml up -d        # or point BULKHEAD_DATABASE_URL at any Postgres
python -m pytest                                        # includes the crash-then-resume test

python -m bulkhead_langgraph.cli --thread demo-1 --reseed
BULKHEAD_CRASH_AFTER_SEND=1 python -m bulkhead_langgraph.cli --thread demo-1 --resume yes   # exit 137
python -m bulkhead_langgraph.cli --thread demo-1                                            # resumes
```

Deploy: `agentcore add credential --name neon --api-key <postgres url>` once, then
`agentcore deploy`. Config is in `agentcore/agentcore.json`; `BULKHEAD_MODEL` picks the model.

Not exercised: AgentCore Memory and Gateway. Identity is used only to hold the database URL.
