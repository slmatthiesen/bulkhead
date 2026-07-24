// Package protocol defines the newline-delimited JSON contract between the control plane and an
// agent subprocess.
//
// It is deliberately the only thing the two halves of bulkhead share. The control plane is Go and
// the agent is Python (or, for tests, a Go fixture); keeping them separate processes with a dumb
// stream between them is what later allows an agent to run inside a different sandbox tier, or on
// a different node, without the control plane changing.
//
// The agent never executes anything itself. It asks for a step to be executed and waits for the
// result. That inversion is what makes checkpoint-and-resume possible: on resume the control plane
// replays recorded outputs for steps it has already executed, so the agent walks back to the same
// internal state without any side effect happening twice.
package protocol

import "encoding/json"

// Message kinds on the wire.
const (
	KindStep   = "step"   // agent -> control plane: execute this
	KindFinal  = "final"  // agent -> control plane: I am done, here is the result
	KindResult = "result" // control plane -> agent: outcome of a step
)

// AgentMessage is anything the agent writes to stdout, one JSON object per line.
type AgentMessage struct {
	Type string `json:"type"`

	// Set when Type is KindStep.
	Seq   int             `json:"seq,omitempty"`
	Step  string          `json:"step,omitempty"` // step kind, e.g. "command"
	Input json.RawMessage `json:"input,omitempty"`

	// Set when Type is KindFinal.
	Patch       string `json:"patch,omitempty"`
	TestsPassed bool   `json:"tests_passed,omitempty"`
}

// StepResult is what the control plane writes to the agent's stdin, one JSON object per line.
type StepResult struct {
	Type   string          `json:"type"`
	Seq    int             `json:"seq"`
	Output json.RawMessage `json:"output,omitempty"`
	Error  string          `json:"error,omitempty"`

	// Replayed reports that this output came from a checkpoint rather than a fresh execution.
	// The agent must behave identically either way; it exists so trajectories are auditable.
	Replayed bool `json:"replayed,omitempty"`
}
