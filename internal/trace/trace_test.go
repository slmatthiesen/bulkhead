package trace

import (
	"context"
	"testing"

	oteltrace "go.opentelemetry.io/otel/trace"
)

// The kill-and-resume story depends on this: two different processes picking up the same run must
// produce spans in one trace, which they only do if the trace id is derived from the run id rather
// than generated. A regression here would not fail any other test — it would just quietly split
// every resumed run into two unrelated traces in the viewer.
func TestRunContextIsStableAcrossProcesses(t *testing.T) {
	first := oteltrace.SpanContextFromContext(RunContext(context.Background(), "run-abc"))
	second := oteltrace.SpanContextFromContext(RunContext(context.Background(), "run-abc"))

	if !first.IsValid() {
		t.Fatal("run context carries no valid span context")
	}
	if first.TraceID() != second.TraceID() {
		t.Errorf("same run produced different trace ids: %s and %s", first.TraceID(), second.TraceID())
	}
	if !first.IsSampled() {
		t.Error("run trace is not sampled, so nothing would be exported")
	}
}

func TestDifferentRunsGetDifferentTraces(t *testing.T) {
	a := oteltrace.SpanContextFromContext(RunContext(context.Background(), "run-a")).TraceID()
	b := oteltrace.SpanContextFromContext(RunContext(context.Background(), "run-b")).TraceID()
	if a == b {
		t.Errorf("distinct runs collapsed into one trace: %s", a)
	}
}
