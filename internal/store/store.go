// Package store is the durable half of the control plane: Postgres holds run state, the step
// checkpoints a resumed run replays from, and the append-only trajectory.
//
// The ordering rules the rest of the system depends on:
//
//   - A step row is written 'pending' before the step executes and updated to 'completed' after,
//     never the other way around. A process that dies mid-step therefore leaves evidence.
//   - Only 'completed' steps are replayed on resume. A 'pending' step is ambiguous — the side
//     effect may or may not have happened — so it is executed again. Closing that window with
//     idempotency keys is Milestone 6.
package store

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

// Run states.
const (
	StateQueued    = "queued"
	StateRunning   = "running"
	StateSucceeded = "succeeded"
	StateFailed    = "failed"
)

// ErrNotFound is returned when a run id does not exist.
var ErrNotFound = errors.New("run not found")

// Run is the durable state of a single agent run.
type Run struct {
	ID          string
	InstanceID  string
	Question    string
	State       string
	Patch       string
	TestsPassed bool
	Error       string
	StepCount   int // completed steps only
}

type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, url string) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	s := &Store{pool: pool}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return s, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) CreateRun(ctx context.Context, instanceID, question string) (Run, error) {
	id, err := newID()
	if err != nil {
		return Run{}, err
	}
	_, err = s.pool.Exec(ctx,
		`insert into runs (id, instance_id, state, question) values ($1, $2, $3, $4)`,
		id, instanceID, StateQueued, question)
	if err != nil {
		return Run{}, fmt.Errorf("create run: %w", err)
	}
	return Run{ID: id, InstanceID: instanceID, Question: question, State: StateQueued}, nil
}

const runColumns = `r.id, r.instance_id, r.question, r.state, r.result_patch, r.result_tests_passed, r.error,
	(select count(*) from steps s where s.run_id = r.id and s.status = 'completed')`

func (s *Store) GetRun(ctx context.Context, id string) (Run, error) {
	var r Run
	err := s.pool.QueryRow(ctx, `select `+runColumns+` from runs r where r.id = $1`, id).
		Scan(&r.ID, &r.InstanceID, &r.Question, &r.State, &r.Patch, &r.TestsPassed, &r.Error, &r.StepCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrNotFound
	}
	if err != nil {
		return Run{}, fmt.Errorf("get run: %w", err)
	}
	return r, nil
}

// UnfinishedRuns returns every run that was queued or in flight, oldest first. A control plane
// calls this on startup: any run still marked running was interrupted by a process death, because
// nothing else can leave a run in that state.
func (s *Store) UnfinishedRuns(ctx context.Context) ([]Run, error) {
	rows, err := s.pool.Query(ctx,
		`select `+runColumns+` from runs r where r.state in ($1, $2) order by r.created_at`,
		StateQueued, StateRunning)
	if err != nil {
		return nil, fmt.Errorf("list unfinished runs: %w", err)
	}
	defer rows.Close()

	var runs []Run
	for rows.Next() {
		var r Run
		if err := rows.Scan(&r.ID, &r.InstanceID, &r.Question, &r.State, &r.Patch, &r.TestsPassed, &r.Error, &r.StepCount); err != nil {
			return nil, fmt.Errorf("scan unfinished run: %w", err)
		}
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

func (s *Store) MarkRunning(ctx context.Context, runID string) error {
	return s.exec(ctx, `update runs set state = $2, updated_at = now() where id = $1`,
		runID, StateRunning)
}

func (s *Store) FinishRun(ctx context.Context, runID, patch string, testsPassed bool) error {
	return s.exec(ctx,
		`update runs set state = $2, result_patch = $3, result_tests_passed = $4, error = '',
		 updated_at = now() where id = $1`,
		runID, StateSucceeded, patch, testsPassed)
}

func (s *Store) FailRun(ctx context.Context, runID, message string) error {
	return s.exec(ctx, `update runs set state = $2, error = $3, updated_at = now() where id = $1`,
		runID, StateFailed, message)
}

// CompletedStep returns the recorded output of a step that already finished. Its second return
// value reports whether such a checkpoint exists; when it does, the step must not run again.
func (s *Store) CompletedStep(ctx context.Context, runID string, seq int) ([]byte, bool, error) {
	var output []byte
	err := s.pool.QueryRow(ctx,
		`select output from steps where run_id = $1 and seq = $2 and status = 'completed'`,
		runID, seq).Scan(&output)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("load checkpoint for step %d: %w", seq, err)
	}
	return output, true, nil
}

// BeginStep durably records the intent to execute a step, before anything happens.
func (s *Store) BeginStep(ctx context.Context, runID string, seq int, kind string, input []byte) error {
	return s.exec(ctx,
		`insert into steps (run_id, seq, kind, status, input) values ($1, $2, $3, 'pending', $4)
		 on conflict (run_id, seq) do update set status = 'pending', input = excluded.input,
		 started_at = now(), completed_at = null, output = null`,
		runID, seq, kind, input)
}

// CompleteStep checkpoints a step's output. Once this returns, the step will never run again.
func (s *Store) CompleteStep(ctx context.Context, runID string, seq int, output []byte) error {
	return s.exec(ctx,
		`update steps set status = 'completed', output = $3, completed_at = now()
		 where run_id = $1 and seq = $2`,
		runID, seq, output)
}

func (s *Store) FailStep(ctx context.Context, runID string, seq int, message string) error {
	payload, _ := jsonString(message)
	return s.exec(ctx,
		`update steps set status = 'failed', output = $3, completed_at = now()
		 where run_id = $1 and seq = $2`,
		runID, seq, payload)
}

// Event is one row of the append-only trajectory. Its json tags are the wire format the
// trajectory viewer consumes.
type Event struct {
	ID         int64           `json:"id"`
	Seq        int             `json:"seq"`
	Kind       string          `json:"kind"`
	Payload    json.RawMessage `json:"payload"`
	RecordedAt time.Time       `json:"at"`
}

// EventsSince returns a run's trajectory events newer than the given id, oldest first. Callers
// tail a run by passing back the last id they saw; the trajectory is append-only, so a reader
// that reconnects after a control-plane death misses nothing.
func (s *Store) EventsSince(ctx context.Context, runID string, after int64) ([]Event, error) {
	rows, err := s.pool.Query(ctx,
		`select id, seq, kind, payload, recorded_at from trajectory_events
		 where run_id = $1 and id > $2 order by id`,
		runID, after)
	if err != nil {
		return nil, fmt.Errorf("list trajectory events: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.Seq, &e.Kind, &e.Payload, &e.RecordedAt); err != nil {
			return nil, fmt.Errorf("scan trajectory event: %w", err)
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

func (s *Store) AppendEvent(ctx context.Context, runID string, seq int, kind string, payload []byte) error {
	if len(payload) == 0 {
		payload = []byte(`null`)
	}
	return s.exec(ctx,
		`insert into trajectory_events (run_id, seq, kind, payload) values ($1, $2, $3, $4)`,
		runID, seq, kind, payload)
}

func (s *Store) exec(ctx context.Context, sql string, args ...any) error {
	if _, err := s.pool.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	return nil
}

func newID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate run id: %w", err)
	}
	return hex.EncodeToString(b), nil
}

func jsonString(s string) ([]byte, error) {
	return []byte(fmt.Sprintf("%q", s)), nil
}

// Step is one checkpoint row, as the under-the-hood view shows it.
type Step struct {
	Seq         int             `json:"seq"`
	Kind        string          `json:"kind"`
	Status      string          `json:"status"`
	Input       json.RawMessage `json:"input"`
	Output      json.RawMessage `json:"output,omitempty"`
	StartedAt   time.Time       `json:"started_at"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
}

// Steps returns a run's checkpoints in order. This is the table resume actually reads from, so
// showing it is the difference between claiming the run was checkpointed and demonstrating it.
func (s *Store) Steps(ctx context.Context, runID string) ([]Step, error) {
	rows, err := s.pool.Query(ctx, `
        select seq, kind, status, input, output, started_at, completed_at
        from steps where run_id = $1 order by seq`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []Step{}
	for rows.Next() {
		var st Step
		if err := rows.Scan(&st.Seq, &st.Kind, &st.Status, &st.Input, &st.Output,
			&st.StartedAt, &st.CompletedAt); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// Pool exposes the connection pool so that things which are part of the control plane's durable
// state -- the queue, notably -- can live in their own package without opening a second pool
// against the same database.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }
