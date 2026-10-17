"""Start, resume, or continue one thread of the graph. Shared by the CLI and the AgentCore entrypoint."""

from __future__ import annotations

from langgraph.types import Command

from . import store
from .graph import build, open_checkpointer


def run_thread(thread_id: str, resume: str | None = None, question: str = "", reseed: bool = False) -> dict:
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
        return out
    out["status"] = "done"
    with store.connect() as conn:
        out["counts"] = store.counts(conn, thread_id)
    return out
