"""The supply-chain agent as a LangGraph graph.

Same workflow as cmd/supplychain-agent: find what is below threshold, group it by vendor, and for
each vendor get a human's approval, send one purchase order, post one Slack notice. Where the Go
version blocks on stdin and lets the control plane answer replayed steps from Postgres, this one
lets LangGraph's Postgres checkpointer do the same job: every node's output is written before the
next node runs, so a run resumed after a crash picks up at the node that was in flight.

The one node that is not safe to re-run blindly is send_po, because its side effect is the point.
It is made safe the same way as in Go: the idempotency key on the purchase_orders row.
"""

from __future__ import annotations

import os
from typing import TypedDict

from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from . import store


class State(TypedDict, total=False):
    run_id: str
    question: str
    stock: list[dict]
    vendors: list[str]
    cursor: int
    explanation: str
    sent: int
    log: list[str]


def _rows_for(state: State, vendor_id: str) -> list[dict]:
    return [r for r in state["stock"] if r["vendor_id"] == vendor_id]


def _current_vendor(state: State) -> str:
    return state["vendors"][state["cursor"]]


def purchase_order(vendor_id: str, rows: list[dict]) -> dict:
    lines = ["Please supply the following at your earliest convenience:", ""]
    items = []
    for r in rows:
        items.append({"sku": r["sku"], "name": r["name"], "quantity": r["reorder_quantity"],
                      "unit_cost_cents": r["unit_cost_cents"]})
        lines.append(f"  {r['sku']:<22} {r['name']}  x{r['reorder_quantity']}")
    lines += ["", f"Current stock is below our reorder threshold on {len(rows)} item(s)."]
    return {
        "vendor_id": vendor_id,
        "vendor_email": rows[0]["vendor_email"],
        "subject": f"Purchase order: {len(rows)} item(s) below reorder threshold",
        "body": "\n".join(lines) + "\n",
        "line_items": items,
        "total_cents": sum(r["reorder_quantity"] * r["unit_cost_cents"] for r in rows),
    }


def query_low_stock(state: State) -> State:
    with store.connect() as conn:
        rows = store.low_stock(conn)
    vendors = sorted({r["vendor_id"] for r in rows})
    return {"stock": rows, "vendors": vendors, "cursor": 0, "sent": 0,
            "log": [f"{len(rows)} products below reorder threshold across {len(vendors)} vendor(s)"]}


def explain(state: State) -> State:
    """Optional model step. The decision stays deterministic; the model only writes the summary,
    and because the checkpointer stores this node's output a resumed run reuses the completion it
    already paid for instead of sampling a new one."""
    if not os.environ.get("BULKHEAD_AGENT_USE_LLM"):
        return {"explanation": ""}
    from .llm import summarise

    return {"explanation": summarise(state["stock"], state.get("question", ""))}


def approve(state: State) -> State:
    vendor_id = _current_vendor(state)
    po = purchase_order(vendor_id, _rows_for(state, vendor_id))
    # The graph stops here and the checkpoint records that it is waiting. Resuming with the
    # operator's reply re-enters this node from the top, which is why nothing above this line may
    # have a side effect.
    reply = interrupt({"vendor_id": vendor_id, "subject": po["subject"], "total_cents": po["total_cents"]})
    with store.connect() as conn:
        store.record_approval(conn, f"{state['run_id']}:{vendor_id}:approve", state["run_id"],
                              vendor_id, po["subject"], po["total_cents"], str(reply))
    return {"log": state["log"] + [f"{vendor_id}: approved ({reply!r})"]}


def send_po(state: State) -> State:
    vendor_id = _current_vendor(state)
    po = purchase_order(vendor_id, _rows_for(state, vendor_id))
    with store.connect() as conn:
        out = store.send_purchase_order(conn, f"{state['run_id']}:{vendor_id}:email", state["run_id"], po)
    if os.environ.get("BULKHEAD_CRASH_AFTER_SEND"):
        # The side effect has happened and the checkpoint for this node has not been written: the
        # exact window the Go demo opens with kill -9.
        os._exit(137)
    note = "duplicate, not re-sent" if out["duplicate"] else "sent"
    return {"log": state["log"] + [f"{vendor_id}: purchase order {out['purchase_order_id']} {note}"]}


def notify(state: State) -> State:
    vendor_id = _current_vendor(state)
    po = purchase_order(vendor_id, _rows_for(state, vendor_id))
    message = f"Purchase order sent to {po['vendor_email']} - ${po['total_cents'] / 100:.2f}"
    with store.connect() as conn:
        po_id = store.purchase_order_id(conn, f"{state['run_id']}:{vendor_id}:email")
        out = store.notify_slack(conn, f"{state['run_id']}:{vendor_id}:slack", state["run_id"], po_id,
                                 "#purchasing", message)
    note = "duplicate" if out["duplicate"] else "posted"
    return {"cursor": state["cursor"] + 1, "sent": state["sent"] + 1,
            "log": state["log"] + [f"{vendor_id}: slack {note}"]}


def after_query(state: State) -> str:
    return "explain" if state["vendors"] else END


def after_notify(state: State) -> str:
    return "approve" if state["cursor"] < len(state["vendors"]) else END


def build(checkpointer):
    g = StateGraph(State)
    g.add_node("query_low_stock", query_low_stock)
    g.add_node("explain", explain)
    g.add_node("approve", approve)
    g.add_node("send_po", send_po)
    g.add_node("notify", notify)
    g.add_edge(START, "query_low_stock")
    g.add_conditional_edges("query_low_stock", after_query)
    g.add_edge("explain", "approve")
    g.add_edge("approve", "send_po")
    g.add_edge("send_po", "notify")
    g.add_conditional_edges("notify", after_notify)
    return g.compile(checkpointer=checkpointer)


def open_checkpointer():
    """LangGraph's checkpoint tables live in the same database as the workload. Returns a context
    manager yielding a PostgresSaver; call setup() once on it."""
    return PostgresSaver.from_conn_string(store.database_url())
