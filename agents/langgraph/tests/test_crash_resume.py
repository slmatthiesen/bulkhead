"""Kill the process after the purchase order is sent but before the checkpoint is written, then
resume. The vendor must end up with exactly one purchase order and one Slack notice."""

import os
import subprocess
import sys
import uuid

import pytest

from bulkhead_langgraph import store

PY = sys.executable


def run(thread: str, *args: str, env: dict | None = None) -> subprocess.CompletedProcess:
    return subprocess.run([PY, "-m", "bulkhead_langgraph.cli", "--thread", thread, *args],
                          capture_output=True, text=True, env={**os.environ, **(env or {})})


@pytest.fixture
def thread():
    with store.connect() as conn:
        store.reseed(conn)
    return f"test-{uuid.uuid4().hex[:8]}"


def test_crash_after_send_then_resume_sends_once(thread):
    first = run(thread)
    assert first.returncode == 2, first.stderr
    assert "awaiting approval" in first.stdout

    crashed = run(thread, "--resume", "yes", env={"BULKHEAD_CRASH_AFTER_SEND": "1"})
    assert crashed.returncode == 137, crashed.stderr
    with store.connect() as conn:
        assert store.counts(conn, thread) == {"approvals": 1, "purchase_orders": 1, "slack_notifications": 0}

    resumed = run(thread)
    assert resumed.returncode == 0, resumed.stderr
    assert "duplicate, not re-sent" in resumed.stdout
    with store.connect() as conn:
        assert store.counts(conn, thread) == {"approvals": 1, "purchase_orders": 1, "slack_notifications": 1}


def test_clean_run_sends_once(thread):
    assert run(thread).returncode == 2
    done = run(thread, "--resume", "looks good")
    assert done.returncode == 0, done.stderr
    with store.connect() as conn:
        assert store.counts(conn, thread) == {"approvals": 1, "purchase_orders": 1, "slack_notifications": 1}
        reply = conn.execute("select reply from approvals where run_id = %s", (thread,)).fetchone()["reply"]
        assert reply == "looks good"
