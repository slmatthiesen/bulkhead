"""Run or resume one thread of the graph from the command line.

    python -m bulkhead_langgraph.cli --thread demo-1              # start; stops at the approval
    python -m bulkhead_langgraph.cli --thread demo-1 --resume yes # answer the approval, continue
    python -m bulkhead_langgraph.cli --thread demo-1              # after a crash: pick up where it died

Exit codes: 0 finished, 2 waiting on an approval.
"""

from __future__ import annotations

import argparse
import json
import sys

from langgraph.types import Command

from . import store
from .graph import build, open_checkpointer


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--thread", required=True)
    p.add_argument("--resume", help="reply to a pending approval")
    p.add_argument("--question", default="")
    p.add_argument("--reseed", action="store_true")
    a = p.parse_args(argv)

    if a.reseed:
        with store.connect() as conn:
            store.reseed(conn)

    config = {"configurable": {"thread_id": a.thread}}
    with open_checkpointer() as cp:
        cp.setup()
        graph = build(cp)
        if a.resume is not None:
            payload = Command(resume=a.resume)
        elif graph.get_state(config).next:
            payload = None  # a checkpoint exists and the run is not finished: continue it
        else:
            payload = {"run_id": a.thread, "question": a.question, "log": []}
        result = graph.invoke(payload, config)
        snapshot = graph.get_state(config)

    for line in result.get("log", []):
        print(line)
    if snapshot.next:
        interrupts = snapshot.tasks[0].interrupts if snapshot.tasks else ()
        print("awaiting approval:", json.dumps(interrupts[0].value if interrupts else None))
        return 2
    with store.connect() as conn:
        print("done:", json.dumps(store.counts(conn, a.thread)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
