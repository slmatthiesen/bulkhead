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
    -- Units consumed per day. Without it "what runs out this week" is unanswerable from stock
    -- levels alone, and a model asked anyway will correctly refuse.
    daily_usage       integer not null default 0,
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

alter table inventory add column if not exists daily_usage integer not null default 0;

-- A second, cheaper side effect after the purchase order, so the demo shows the idempotency pattern
-- applied twice rather than looking like a one-off trick for email. Same shape as purchase_orders:
-- idempotency_key is unique, so a retried notification collides here instead of paging the channel
-- twice.
create table if not exists slack_notifications (
    id                bigserial primary key,
    idempotency_key   text not null unique,
    run_id            text not null,
    purchase_order_id bigint not null references purchase_orders (id),
    channel           text not null,
    message           text not null,
    sent_at           timestamptz not null default now()
);

create index if not exists slack_notifications_run_idx on slack_notifications (run_id, id);

-- A human approval gate before the purchase order goes out. The row is the wait: the step blocks
-- until this row's status flips to 'approved', and it is looked up by idempotency_key on retry so a
-- run resumed after a crash finds the same pending (or already-approved) decision instead of asking
-- again.
create table if not exists approvals (
    id              bigserial primary key,
    idempotency_key text not null unique,
    run_id          text not null,
    vendor_id       text not null,
    subject         text not null,
    total_cents     integer not null,
    status          text not null default 'pending' check (status in ('pending', 'approved')),
    -- What the operator actually typed. The gate is "did a human reply", not "did they say a
    -- specific word" -- this column is what makes that an honest claim instead of a fake one.
    reply           text,
    decided_at      timestamptz
);

create index if not exists approvals_run_idx on approvals (run_id, id);

alter table approvals add column if not exists reply text;
