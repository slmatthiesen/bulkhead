"""The zip deployed to AgentCore cannot see the Go tree, so it carries a copy of the workload SQL.
This keeps the copy honest."""

from bulkhead_langgraph import store


def test_packaged_sql_matches_go_tree():
    for name in ("schema.sql", "seed.sql"):
        assert (store.PACKAGED_SQL_DIR / name).read_bytes() == (store.GO_WORKLOAD_DIR / name).read_bytes(), name
