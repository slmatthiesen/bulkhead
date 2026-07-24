// Package run drives a single agent run to completion, and back to completion again if the
// process running it dies partway through.
package run

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"

	"github.com/slmatthiesen/bulkhead/internal/executor"
	"github.com/slmatthiesen/bulkhead/internal/protocol"
	"github.com/slmatthiesen/bulkhead/internal/store"
)

// Engine executes runs. One Engine serves every run in the process; Milestone 1 runs them
// serially, and concurrency arrives with the worker pool in Milestone 3.
type Engine struct {
	store    *store.Store
	exec     executor.Executor
	agentCmd []string
	log      *slog.Logger
}

func NewEngine(s *store.Store, e executor.Executor, agentCmd []string, log *slog.Logger) *Engine {
	return &Engine{store: s, exec: e, agentCmd: agentCmd, log: log}
}

// Execute drives a run from wherever it currently is to a terminal state.
//
// Resuming is not a special case with its own code path. The agent is restarted from scratch and
// asks for the same steps in the same order; steps that already have a checkpoint are answered
// from Postgres without executing, so the agent walks back to the state it had when the process
// died and then carries on. A resumed run and an uninterrupted run take the same path through
// this function.
func (e *Engine) Execute(ctx context.Context, r store.Run) error {
	log := e.log.With("run", r.ID, "instance", r.InstanceID)
	if err := e.store.MarkRunning(ctx, r.ID); err != nil {
		return err
	}
	// Every attempt at a run appends its own start event, so the trajectory shows how many times
	// the run was picked up and how much of it was already checkpointed at that moment.
	started, _ := json.Marshal(map[string]int{"checkpointed_steps": r.StepCount})
	if err := e.store.AppendEvent(ctx, r.ID, 0, "run_started", started); err != nil {
		return err
	}

	agent := exec.CommandContext(ctx, e.agentCmd[0], e.agentCmd[1:]...)
	agent.Env = append(os.Environ(),
		"BULKHEAD_RUN_ID="+r.ID,
		"BULKHEAD_INSTANCE_ID="+r.InstanceID,
	)
	stdin, err := agent.StdinPipe()
	if err != nil {
		return e.fail(ctx, r.ID, fmt.Errorf("agent stdin: %w", err))
	}
	stdout, err := agent.StdoutPipe()
	if err != nil {
		return e.fail(ctx, r.ID, fmt.Errorf("agent stdout: %w", err))
	}
	agent.Stderr = &logWriter{log: log}

	if err := agent.Start(); err != nil {
		return e.fail(ctx, r.ID, fmt.Errorf("start agent: %w", err))
	}
	log.Info("agent started", "pid", agent.Process.Pid)

	err = e.pump(ctx, r, stdin, stdout, log)
	stdin.Close()
	if waitErr := agent.Wait(); waitErr != nil && err == nil {
		err = fmt.Errorf("agent exited: %w", waitErr)
	}
	if err != nil {
		return e.fail(ctx, r.ID, err)
	}
	return nil
}

// pump is the conversation with the agent: it asks for steps, we answer them, and eventually it
// reports a result.
func (e *Engine) pump(ctx context.Context, r store.Run, stdin io.Writer, stdout io.Reader, log *slog.Logger) error {
	replies := json.NewEncoder(stdin)
	messages := bufio.NewScanner(stdout)
	messages.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	for messages.Scan() {
		var msg protocol.AgentMessage
		if err := json.Unmarshal(messages.Bytes(), &msg); err != nil {
			return fmt.Errorf("decode agent message %q: %w", messages.Text(), err)
		}

		switch msg.Type {
		case protocol.KindStep:
			result, err := e.resolveStep(ctx, r.ID, msg, log)
			if err != nil {
				return err
			}
			if err := replies.Encode(result); err != nil {
				return fmt.Errorf("reply to step %d: %w", msg.Seq, err)
			}
			if result.Error != "" {
				return fmt.Errorf("step %d failed: %s", msg.Seq, result.Error)
			}

		case protocol.KindFinal:
			payload, _ := json.Marshal(msg)
			if err := e.store.AppendEvent(ctx, r.ID, 0, "run_finished", payload); err != nil {
				return err
			}
			log.Info("run finished", "tests_passed", msg.TestsPassed)
			return e.store.FinishRun(ctx, r.ID, msg.Patch, msg.TestsPassed)

		default:
			return fmt.Errorf("unknown agent message type %q", msg.Type)
		}
	}
	if err := messages.Err(); err != nil {
		return fmt.Errorf("read from agent: %w", err)
	}
	return errors.New("agent closed its output without reporting a result")
}

// resolveStep answers one step request, either from a checkpoint or by actually executing it.
func (e *Engine) resolveStep(ctx context.Context, runID string, msg protocol.AgentMessage, log *slog.Logger) (protocol.StepResult, error) {
	if output, ok, err := e.store.CompletedStep(ctx, runID, msg.Seq); err != nil {
		return protocol.StepResult{}, err
	} else if ok {
		log.Info("replaying checkpointed step", "seq", msg.Seq)
		if err := e.store.AppendEvent(ctx, runID, msg.Seq, "step_replayed", output); err != nil {
			return protocol.StepResult{}, err
		}
		return protocol.StepResult{
			Type:     protocol.KindResult,
			Seq:      msg.Seq,
			Output:   output,
			Replayed: true,
		}, nil
	}

	// Intent first, so that a process death between here and the checkpoint below is visible
	// rather than silent.
	if err := e.store.BeginStep(ctx, runID, msg.Seq, msg.Step, msg.Input); err != nil {
		return protocol.StepResult{}, err
	}
	if err := e.store.AppendEvent(ctx, runID, msg.Seq, "step_started", msg.Input); err != nil {
		return protocol.StepResult{}, err
	}

	output, execErr := e.exec.Execute(ctx, executor.Request{
		RunID:          runID,
		Seq:            msg.Seq,
		Kind:           msg.Step,
		Input:          msg.Input,
		IdempotencyKey: fmt.Sprintf("%s:%d", runID, msg.Seq),
	})
	if execErr != nil {
		if err := e.store.FailStep(ctx, runID, msg.Seq, execErr.Error()); err != nil {
			return protocol.StepResult{}, err
		}
		return protocol.StepResult{Type: protocol.KindResult, Seq: msg.Seq, Error: execErr.Error()}, nil
	}

	if err := e.store.CompleteStep(ctx, runID, msg.Seq, output); err != nil {
		return protocol.StepResult{}, err
	}
	if err := e.store.AppendEvent(ctx, runID, msg.Seq, "step_completed", output); err != nil {
		return protocol.StepResult{}, err
	}
	return protocol.StepResult{Type: protocol.KindResult, Seq: msg.Seq, Output: output}, nil
}

func (e *Engine) fail(ctx context.Context, runID string, cause error) error {
	// The run failed for its own reasons; a failure to write that down is a different problem and
	// should not replace the original error.
	reason, _ := json.Marshal(cause.Error())
	if err := e.store.AppendEvent(ctx, runID, 0, "run_failed", reason); err != nil {
		e.log.Error("could not record run failure event", "run", runID, "err", err)
	}
	if err := e.store.FailRun(ctx, runID, cause.Error()); err != nil {
		e.log.Error("could not record run failure", "run", runID, "err", err)
	}
	return cause
}

// logWriter forwards the agent's stderr into the control plane's log, line by line.
type logWriter struct {
	log *slog.Logger
}

func (w *logWriter) Write(p []byte) (int, error) {
	for _, line := range strings.Split(strings.TrimRight(string(p), "\n"), "\n") {
		if line != "" {
			w.log.Info("agent", "msg", line)
		}
	}
	return len(p), nil
}
