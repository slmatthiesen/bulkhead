"""Start, resume, or continue one thread of the graph. Shared by the CLI and the AgentCore entrypoint."""

from __future__ import annotations

import time

from langgraph.types import Command

from . import costs, store
from .graph import build, open_checkpointer


def run_thread(thread_id: str, resume: str | None = None, question: str = "", reseed: bool = False) -> dict:
    started = time.monotonic()
    if reseed:
        with store.connect() as conn:
            store.reseed(conn)

    config = {"configurable": {"thread_id": thread_id}}
    with open_checkpointer() as cp:
        cp.setup()
        graph = build(cp)
        if resume is not None:
            payload = Command(resume=resume)
        elif graph.get_state(config).next:
            payload = None  # a checkpoint exists and the run is not finished: continue it
        else:
            payload = {"run_id": thread_id, "question": question, "log": []}
        result = graph.invoke(payload, config)
        snapshot = graph.get_state(config)

    out = {"thread_id": thread_id, "log": result.get("log", []), "explanation": result.get("explanation", "")}
    if snapshot.next:
        interrupts = snapshot.tasks[0].interrupts if snapshot.tasks else ()
        out["status"] = "awaiting_approval"
        out["pending"] = interrupts[0].value if interrupts else None
    else:
        out["status"] = "done"

    # The model is only called on the first invocation of a thread; a resumed run replays the
    # stored completion, so later rows for the same run_id carry zero tokens. That is the point.
    wall_ms = int((time.monotonic() - started) * 1000)
    with store.connect() as conn:
        first_invocation = costs.totals(conn, thread_id)["invocations"] == 0
        out["cost"] = costs.record(conn, thread_id, out["explanation"] if first_invocation else "", wall_ms, out["status"])
        if out["status"] == "done":
            out["counts"] = store.counts(conn, thread_id)
            out["cost_total"] = costs.totals(conn, thread_id)
    return out
