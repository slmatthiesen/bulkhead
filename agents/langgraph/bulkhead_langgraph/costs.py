"""Cost per run. One row per invocation: which model, how many tokens, how long, and what that
cost in dollars at Bedrock's on-demand rates. The Runtime's own vCPU-seconds are billed per second
with a 1-second minimum, so wall time is the proxy recorded here and the README costs it out.

Prices are USD per 1,000,000 tokens, us-west-2 on-demand, read from the Bedrock pricing page on
2026-10-17. Update the table, not the code, when they move.
"""

from __future__ import annotations

import psycopg

PRICE_PER_MTOK = {
    "us.amazon.nova-pro-v1:0": (0.80, 3.20),
    "us.anthropic.claude-sonnet-4-5-20250929-v1:0": (3.00, 15.00),
}

SCHEMA = """
create table if not exists run_costs (
    id            bigserial primary key,
    run_id        text not null,
    invoked_at    timestamptz not null default now(),
    model         text,
    input_tokens  integer not null default 0,
    output_tokens integer not null default 0,
    wall_ms       integer not null,
    model_usd     numeric(12, 8) not null default 0,
    status        text not null
);
create index if not exists run_costs_run_idx on run_costs (run_id, id);
"""


def model_usd(model: str | None, input_tokens: int, output_tokens: int) -> float:
    if not model or model not in PRICE_PER_MTOK:
        return 0.0
    cin, cout = PRICE_PER_MTOK[model]
    return (input_tokens * cin + output_tokens * cout) / 1_000_000


def record(conn: psycopg.Connection, run_id: str, explanation: dict | str, wall_ms: int, status: str) -> dict:
    conn.execute(SCHEMA)
    usage = explanation if isinstance(explanation, dict) else {}
    model = usage.get("model")
    inp, out = int(usage.get("input_tokens", 0)), int(usage.get("output_tokens", 0))
    usd = model_usd(model, inp, out)
    conn.execute(
        """insert into run_costs (run_id, model, input_tokens, output_tokens, wall_ms, model_usd, status)
           values (%s, %s, %s, %s, %s, %s, %s)""",
        (run_id, model, inp, out, wall_ms, usd, status),
    )
    return {"model": model, "input_tokens": inp, "output_tokens": out, "wall_ms": wall_ms, "model_usd": round(usd, 6)}


def totals(conn: psycopg.Connection, run_id: str) -> dict:
    conn.execute(SCHEMA)
    row = conn.execute(
        """select count(*) as invocations, coalesce(sum(wall_ms),0) as wall_ms,
                  coalesce(sum(input_tokens),0) as input_tokens, coalesce(sum(output_tokens),0) as output_tokens,
                  coalesce(sum(model_usd),0) as model_usd
           from run_costs where run_id = %s""",
        (run_id,),
    ).fetchone()
    return {k: (float(v) if k == "model_usd" else int(v)) for k, v in row.items()}
