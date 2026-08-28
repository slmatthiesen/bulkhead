// Package queue is how work reaches a worker.
//
// It replaced an in-process channel plus a separate startup scan for runs orphaned by a crash.
// Those were two mechanisms doing one job, and the second existed only because the first could not
// survive the process. A leased queue collapses them: a claimed message becomes invisible for the
// length of its lease, and a worker that dies without acknowledging it simply lets the lease
// expire, at which point the message is delivered again. Recovery is not a special case — it is
// what redelivery already does.
//
// That leaves exactly one path into the harness, which is the property worth having. Every run is
// enqueued and every run is claimed, whether it is new or being retried after a crash, so there is
// one place where duplicate delivery can happen and one place it has to be handled.
//
// Delivery is at-least-once, deliberately. Exactly-once delivery is not available to anything that
// talks to a network, so the harness assumes duplicates and makes them harmless: steps are
// checkpointed before they run, completed steps are replayed rather than re-executed, and side
// effects carry an idempotency key. A queue promising exactly-once would hide the problem rather
// than solve it.
//
// The Postgres implementation is here because the checkpoints are already in Postgres and a second
// piece of infrastructure would earn nothing at this scale. The semantics are deliberately the ones
// SQS has -- a visibility timeout, a receive count, and no ordering guarantee beyond best effort --
// so that swapping the implementation is a configuration change and not a redesign.
package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Message is one unit of claimed work.
type Message struct {
	RunID string

	// Deliveries counts how many times this message has been handed to a worker, including this
	// one. Anything above 1 means a previous worker took it and did not finish -- the number is
	// the poison-message signal, and Milestone 6 is where it starts routing to a dead-letter queue
	// instead of being retried forever.
	Deliveries int
}

// Queue hands work to workers and takes it back if they die holding it.
type Queue interface {
	// Enqueue makes a run available to be claimed. Enqueueing a run that is already queued is a
	// no-op rather than an error, so a caller retrying a request cannot create two runs of it.
	Enqueue(ctx context.Context, runID string) error

	// Claim leases the next available message, or reports false if there is nothing to do.
	Claim(ctx context.Context) (Message, bool, error)

	// Extend pushes a lease further out, for work still in progress.
	Extend(ctx context.Context, runID string) error

	// Ack removes a message permanently. Until this is called the message comes back.
	Ack(ctx context.Context, runID string) error
}

// name scopes a queue so that independent sets of workers sharing one database do not claim each
// other's messages. Workers sharing work is the entire point of a queue, and it is also how a demo
// left running quietly eats the acceptance test's messages and runs them with the wrong agent --
// which fails in a way that looks like a bug in the harness rather than two things talking to one
// table.
const schema = `
create table if not exists queue (
    run_id      text primary key references runs (id) on delete cascade,
    name        text not null default 'default',
    enqueued_at timestamptz not null default now(),
    visible_at  timestamptz not null default now(),
    deliveries  integer not null default 0
);
alter table queue add column if not exists name text not null default 'default';
create index if not exists queue_visible_idx on queue (name, visible_at, enqueued_at);`

// Postgres is a leased queue over the database that already holds the checkpoints.
type Postgres struct {
	pool *pgxpool.Pool
	name string

	// lease is how long a worker may hold a message before it is assumed dead. Too short and a
	// slow run is delivered to a second worker while the first is still working on it -- which is
	// survivable, because duplicate delivery is survivable, but wasteful. Too long and a genuine
	// crash leaves work stalled for that long.
	lease time.Duration
}

func NewPostgres(ctx context.Context, pool *pgxpool.Pool, name string, lease time.Duration) (*Postgres, error) {
	if name == "" {
		name = "default"
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		return nil, fmt.Errorf("queue schema: %w", err)
	}
	return &Postgres{pool: pool, name: name, lease: lease}, nil
}

func (q *Postgres) Enqueue(ctx context.Context, runID string) error {
	_, err := q.pool.Exec(ctx, `
        insert into queue (run_id, name) values ($1, $2)
        on conflict (run_id) do nothing`, runID, q.name)
	if err != nil {
		return fmt.Errorf("enqueue %s: %w", runID, err)
	}
	return nil
}

// Claim takes the oldest visible message and hides it for the lease duration.
//
// FOR UPDATE SKIP LOCKED is what makes this safe for more than one worker: concurrent claimers
// step over rows their peers have locked instead of blocking on them, so N workers pull N distinct
// messages without coordinating. That is the same property Milestone 3 needs from a real broker,
// which is why the interface is shaped this way now rather than later.
func (q *Postgres) Claim(ctx context.Context) (Message, bool, error) {
	var msg Message
	err := q.pool.QueryRow(ctx, `
        update queue set visible_at = now() + $1::interval, deliveries = deliveries + 1
        where run_id = (
            select run_id from queue
            where name = $2 and visible_at <= now()
            order by enqueued_at
            for update skip locked
            limit 1
        )
        returning run_id, deliveries`, q.lease.String(), q.name).Scan(&msg.RunID, &msg.Deliveries)

	if errors.Is(err, pgx.ErrNoRows) {
		return Message{}, false, nil
	}
	if err != nil {
		return Message{}, false, fmt.Errorf("claim: %w", err)
	}
	return msg, true, nil
}

func (q *Postgres) Extend(ctx context.Context, runID string) error {
	_, err := q.pool.Exec(ctx,
		`update queue set visible_at = now() + $1::interval where run_id = $2`, q.lease.String(), runID)
	if err != nil {
		return fmt.Errorf("extend lease on %s: %w", runID, err)
	}
	return nil
}

func (q *Postgres) Ack(ctx context.Context, runID string) error {
	if _, err := q.pool.Exec(ctx, `delete from queue where run_id = $1`, runID); err != nil {
		return fmt.Errorf("ack %s: %w", runID, err)
	}
	return nil
}

// Depth reports how many messages are waiting, which is the number KEDA scales on in Milestone 4
// and the number worth putting on a dashboard well before then.
func (q *Postgres) Depth(ctx context.Context) (waiting, leased int, err error) {
	err = q.pool.QueryRow(ctx, `
        select count(*) filter (where visible_at <= now()),
               count(*) filter (where visible_at >  now())
        from queue where name = $1`, q.name).Scan(&waiting, &leased)
	return
}
