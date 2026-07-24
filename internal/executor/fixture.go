package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// Fixture is an executor for the acceptance test. It performs a visible, recorded side effect —
// appending to a ledger file — so a test can tell the difference between a control plane that
// replayed a checkpointed step and one that quietly ran it a second time.
//
// It appends unconditionally, including on retry. Deduplicating here would hide exactly the
// behaviour the acceptance test is there to observe.
type Fixture struct {
	ledgerPath string
	delay      time.Duration
	mu         sync.Mutex
}

func NewFixture(ledgerPath string, delay time.Duration) *Fixture {
	return &Fixture{ledgerPath: ledgerPath, delay: delay}
}

func (f *Fixture) Execute(ctx context.Context, req Request) (json.RawMessage, error) {
	cmd, err := decodeCommand(req.Input)
	if err != nil {
		return nil, err
	}
	if err := f.record(req.IdempotencyKey); err != nil {
		return nil, err
	}

	// The delay is what gives a test a wide enough window to kill the control plane mid-run.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(f.delay):
	}

	echoed, ok := strings.CutPrefix(cmd.Command, "echo ")
	if !ok {
		return nil, fmt.Errorf("fixture executor only understands echo, got %q", cmd.Command)
	}
	return json.Marshal(echoed)
}

func (f *Fixture) Close() error { return nil }

func (f *Fixture) record(key string) error {
	if f.ledgerPath == "" {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	file, err := os.OpenFile(f.ledgerPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open fixture ledger: %w", err)
	}
	defer file.Close()
	if _, err := fmt.Fprintln(file, key); err != nil {
		return fmt.Errorf("write fixture ledger: %w", err)
	}
	return file.Sync()
}
