// Command fixture-agent is a deterministic stand-in for the real Python agent.
//
// It speaks the same protocol over the same pipes, so the acceptance test exercises the real
// subprocess boundary — but it always asks for the same steps in the same order, which is what
// makes "an interrupted run produces identical output" a claim about the control plane's replay
// rather than about model temperature.
//
// Its final patch is derived from the step outputs it received, so if replay ever handed back the
// wrong output for a step, the patch would differ and the acceptance test would catch it.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/slmatthiesen/bulkhead/internal/protocol"
)

const stepCount = 6

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fixture-agent:", err)
		os.Exit(1)
	}
}

func run() error {
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	out := json.NewEncoder(os.Stdout)

	outputs := make([]string, 0, stepCount)
	for seq := 1; seq <= stepCount; seq++ {
		input, _ := json.Marshal(map[string]string{
			"command": fmt.Sprintf("echo bulkhead-fixture-step-%d", seq),
		})
		req := protocol.AgentMessage{
			Type:  protocol.KindStep,
			Seq:   seq,
			Step:  "command",
			Input: input,
		}
		if err := out.Encode(req); err != nil {
			return fmt.Errorf("write step %d: %w", seq, err)
		}

		if !in.Scan() {
			if err := in.Err(); err != nil {
				return fmt.Errorf("read result %d: %w", seq, err)
			}
			return fmt.Errorf("control plane closed the stream before step %d", seq)
		}
		var res protocol.StepResult
		if err := json.Unmarshal(in.Bytes(), &res); err != nil {
			return fmt.Errorf("decode result %d: %w", seq, err)
		}
		if res.Error != "" {
			return fmt.Errorf("step %d failed: %s", seq, res.Error)
		}
		if res.Seq != seq {
			return fmt.Errorf("out of order result: want seq %d, got %d", seq, res.Seq)
		}
		outputs = append(outputs, string(res.Output))
	}

	return out.Encode(protocol.AgentMessage{
		Type:        protocol.KindFinal,
		Patch:       patchFrom(outputs),
		TestsPassed: true,
	})
}

// patchFrom builds a deterministic patch out of every step output, so the acceptance test's
// comparison covers the content of replayed steps and not merely the fact that a run finished.
func patchFrom(outputs []string) string {
	var b strings.Builder
	b.WriteString("--- a/fixture.txt\n+++ b/fixture.txt\n")
	for _, o := range outputs {
		fmt.Fprintf(&b, "+%s\n", strings.TrimSpace(o))
	}
	return b.String()
}
