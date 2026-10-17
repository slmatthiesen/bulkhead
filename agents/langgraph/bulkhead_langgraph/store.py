"""The supply-chain workload's tables, shared with the Go demo.

Schema and seed are read from the Go tree so the two implementations cannot drift: the same
purchase_orders table, the same unique idempotency_key, the same vendors and stock.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

DEFAULT_URL = "postgresql://bulkhead:bulkhead@127.0.0.1:5436/bulkhead"

WORKLOAD_DIR = Path(__file__).resolve().parents[3] / "internal" / "workload" / "supplychain"

INVENTORY_SELECT = """
    select i.sku, i.name, i.category, i.on_hand, i.reorder_threshold, i.reorder_quantity,
           i.unit_cost_cents, i.daily_usage, i.vendor_id, v.name as vendor_name, v.email as vendor_email
    from inventory i join vendors v on v.id = i.vendor_id
"""


def database_url() -> str:
    return os.environ.get("BULKHEAD_DATABASE_URL", DEFAULT_URL)


def connect() -> psycopg.Connection:
    return psycopg.connect(database_url(), row_factory=dict_row, autocommit=True)


def reseed(conn: psycopg.Connection) -> None:
    conn.execute((WORKLOAD_DIR / "schema.sql").read_text(encoding="utf-8"))
    for table in ("slack_notifications", "approvals", "purchase_orders"):
        conn.execute(f"delete from {table}")
    conn.execute((WORKLOAD_DIR / "seed.sql").read_text(encoding="utf-8"))


def low_stock(conn: psycopg.Connection) -> list[dict]:
    sql = INVENTORY_SELECT + " where i.on_hand < i.reorder_threshold order by i.on_hand::float / i.reorder_threshold"
    return conn.execute(sql).fetchall()


def record_approval(conn: psycopg.Connection, key: str, run_id: str, vendor_id: str,
                    subject: str, total_cents: int, reply: str) -> None:
    conn.execute(
        """insert into approvals (idempotency_key, run_id, vendor_id, subject, total_cents, status, reply, decided_at)
           values (%s, %s, %s, %s, %s, 'approved', %s, now())
           on conflict (idempotency_key) do nothing""",
        (key, run_id, vendor_id, subject, total_cents, reply),
    )


def send_purchase_order(conn: psycopg.Connection, key: str, run_id: str, po: dict) -> dict:
    """The insert is the send. A retry of a step that already sent collides on idempotency_key and
    is reported as a duplicate instead of ordering twice."""
    row = conn.execute(
        """insert into purchase_orders
               (idempotency_key, run_id, vendor_id, vendor_email, subject, body, line_items, total_cents)
           values (%s, %s, %s, %s, %s, %s, %s, %s)
           on conflict (idempotency_key) do nothing
           returning id""",
        (key, run_id, po["vendor_id"], po["vendor_email"], po["subject"], po["body"],
         json.dumps(po["line_items"]), po["total_cents"]),
    ).fetchone()
    if row:
        return {"purchase_order_id": row["id"], "duplicate": False}
    row = conn.execute("select id from purchase_orders where idempotency_key = %s", (key,)).fetchone()
    return {"purchase_order_id": row["id"], "duplicate": True}


def purchase_order_id(conn: psycopg.Connection, key: str) -> int:
    return conn.execute("select id from purchase_orders where idempotency_key = %s", (key,)).fetchone()["id"]


def notify_slack(conn: psycopg.Connection, key: str, run_id: str, po_id: int, channel: str, message: str) -> dict:
    row = conn.execute(
        """insert into slack_notifications (idempotency_key, run_id, purchase_order_id, channel, message)
           values (%s, %s, %s, %s, %s)
           on conflict (idempotency_key) do nothing
           returning id""",
        (key, run_id, po_id, channel, message),
    ).fetchone()
    return {"channel": channel, "duplicate": row is None}


def counts(conn: psycopg.Connection, run_id: str) -> dict:
    return {
        table: conn.execute(f"select count(*) as n from {table} where run_id = %s", (run_id,)).fetchone()["n"]
        for table in ("approvals", "purchase_orders", "slack_notifications")
    }
