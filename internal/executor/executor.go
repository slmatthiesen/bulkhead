// Package executor runs a single agent step somewhere isolated and returns what it produced.
//
// One interface, and for now one real implementation plus a test fixture. The interface exists
// from the start because Milestone 2 adds gVisor, Firecracker, and WASM tiers behind it, and the
// control plane must not learn anything about which tier it is talking to.
package executor

import (
	"context"
	"encoding/json"
	"fmt"
)

// Request is one step to execute.
type Request struct {
	RunID string
	Seq   int
	Kind  string
	Input json.RawMessage

	// IdempotencyKey identifies this step across retries. A step that was in flight when the
	// control plane died is retried on resume, and an executor that can deduplicate should use
	// this to avoid repeating the side effect.
	IdempotencyKey string
}

// Executor runs steps. Implementations must be safe for concurrent use.
type Executor interface {
	Execute(ctx context.Context, req Request) (json.RawMessage, error)
	Close() error
}

// Command is the only step kind in Milestone 1.
const KindCommand = "command"

// CommandInput is the payload of a KindCommand step.
type CommandInput struct {
	Command string `json:"command"`
}

func decodeCommand(input json.RawMessage) (CommandInput, error) {
	var cmd CommandInput
	if err := json.Unmarshal(input, &cmd); err != nil {
		return CommandInput{}, fmt.Errorf("decode command input: %w", err)
	}
	if cmd.Command == "" {
		return CommandInput{}, fmt.Errorf("command step has no command")
	}
	return cmd, nil
}
