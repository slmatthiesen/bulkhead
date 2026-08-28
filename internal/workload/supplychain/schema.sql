-- The demo workload: a warehouse that runs low on stock and an agent that reorders it.
--
-- This exists to make the harness legible to someone who does not read Go. The SWE-bench workload
-- is what justifies the sandbox and produces the benchmark; this one is what you can watch.

create table if not exists vendors (
    id             text primary key,
    name           text not null,
    email          text not null,
    lead_time_days integer not null
);

create table if not exists inventory (
    sku               text primary key,
    name              text not null,
    category          text not null,
    on_hand           integer not null,
    reorder_threshold integer not null,
    reorder_quantity  integer not null,
    unit_cost_cents   integer not null,
    vendor_id         text not null references vendors (id)
);

-- The outbox. Ordering from a vendor is this workload's real side effect, and a real side effect is
-- the entire reason checkpointing matters -- replaying a step that reads inventory costs nothing,
-- replaying one that sends a purchase order costs money and embarrasses you.
--
-- Nothing is actually emailed. The row IS the send: visible, auditable, and safe to run on a laptop
-- in front of an audience.
--
-- idempotency_key is unique, and that constraint is the demo. The control plane already derives a
-- key per step (run id and sequence number), so a step that was in flight when the process died and
-- is retried on resume collides here instead of ordering twice. Kill the control plane mid-send and
-- the vendor still receives exactly one purchase order.
create table if not exists purchase_orders (
    id              bigserial primary key,
    idempotency_key text not null unique,
    run_id          text not null,
    vendor_id       text not null,
    vendor_email    text not null,
    subject         text not null,
    body            text not null,
    line_items      jsonb not null,
    total_cents     integer not null,
    sent_at         timestamptz not null default now()
);

create index if not exists purchase_orders_run_idx on purchase_orders (run_id, id);
