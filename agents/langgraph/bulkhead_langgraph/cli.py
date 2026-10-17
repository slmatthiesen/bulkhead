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

from .runner import run_thread


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--thread", required=True)
    p.add_argument("--resume", help="reply to a pending approval")
    p.add_argument("--question", default="")
    p.add_argument("--reseed", action="store_true")
    a = p.parse_args(argv)

    out = run_thread(a.thread, resume=a.resume, question=a.question, reseed=a.reseed)
    for line in out["log"]:
        print(line)
    if out["explanation"]:
        print("model:", out["explanation"])
    if out["status"] == "awaiting_approval":
        print("awaiting approval:", json.dumps(out["pending"]))
        return 2
    print("done:", json.dumps(out["counts"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
