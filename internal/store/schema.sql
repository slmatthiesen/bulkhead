create table if not exists runs (
    id                  text primary key,
    instance_id         text not null,
    -- What the operator actually asked. The agent puts it in front of the model, so the run is
    -- answering a question rather than executing a script that happens to be scheduled.
    question            text not null default '',
    state               text not null,
    result_patch        text not null default '',
    result_tests_passed boolean not null default false,
    error               text not null default '',
    created_at          timestamptz not null default now(),
    updated_at          timestamptz not null default now()
);

-- One row per step the agent asked for. The row is written as 'pending' before the step executes
-- and updated to 'completed' with its output after, so a control plane that dies mid-step leaves
-- behind a durable record that the step was attempted.
create table if not exists steps (
    run_id       text not null references runs (id) on delete cascade,
    seq          integer not null,
    kind         text not null,
    status       text not null,
    input        jsonb not null,
    output       jsonb,
    started_at   timestamptz not null default now(),
    completed_at timestamptz,
    primary key (run_id, seq)
);

create index if not exists steps_run_status_idx on steps (run_id, status);

-- Append-only trajectory. Nothing reads it yet; it exists because the checkpoint story and the
-- event log are the same story, and Milestone 5 moves this stream to Kafka partitions.
create table if not exists trajectory_events (
    id          bigserial primary key,
    run_id      text not null references runs (id) on delete cascade,
    seq         integer not null,
    kind        text not null,
    payload     jsonb not null,
    recorded_at timestamptz not null default now()
);

create index if not exists trajectory_events_run_idx on trajectory_events (run_id, id);

-- Additive migrations. `create table if not exists` above does nothing to a database that already
-- has the table, so a column added after the fact has to say so explicitly.
alter table runs add column if not exists question text not null default '';
