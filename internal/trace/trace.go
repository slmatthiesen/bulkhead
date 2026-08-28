// Package trace exports the control plane's spans over OTLP.
//
// Langfuse is the intended consumer — it ingests OTLP directly — but nothing in this package knows
// that. An OTLP endpoint is an OTLP endpoint, so a collector, Jaeger or Honeycomb works unchanged.
// Configuration is the standard OTEL_* environment, so there is no bulkhead-specific vocabulary to
// learn and no provider hard-coded anywhere.
//
// Tracing is off unless OTEL_EXPORTER_OTLP_ENDPOINT is set. The acceptance test runs with it unset
// and therefore costs nothing and needs no collector.
package trace

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const scope = "github.com/slmatthiesen/bulkhead"

// Tracer returns the control plane's tracer. It is a no-op tracer until Setup installs a provider,
// so callers can be written without a nil check and without caring whether tracing is configured.
func Tracer() oteltrace.Tracer { return otel.Tracer(scope) }

// Setup installs the global tracer provider and returns its shutdown function. When no OTLP
// endpoint is configured it installs nothing and returns a no-op shutdown.
func Setup(ctx context.Context, log *slog.Logger) (func(context.Context) error, error) {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		log.Info("tracing disabled", "reason", "OTEL_EXPORTER_OTLP_ENDPOINT is unset")
		return func(context.Context) error { return nil }, nil
	}

	// The client reads endpoint, headers and protocol from the standard OTEL_* environment.
	// Langfuse wants the OTLP path on its API host and a basic-auth header built from the project
	// keys; both are expressed entirely in that environment, not here.
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}

	// Schemaless deliberately: merging a versioned resource against resource.Default() fails
	// whenever the SDK's schema version moves ahead of the semconv package, which is a build-time
	// dependency bump turning into a runtime startup failure.
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		semconv.ServiceName("bulkhead-control-plane"),
	))
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}

	// A short batch window, deliberately, against the SDK's 5s default.
	//
	// This is a harness whose entire subject is processes dying. Spans still sitting in the batch
	// processor when the control plane is killed are lost, and those are precisely the spans worth
	// having — the ones describing the step that was in flight. Flushing every second shrinks that
	// window to about a second's worth of spans.
	//
	// It does not close it. The durable record of a trajectory is Postgres, not the exporter; the
	// honest fix is to export spans from the trajectory_events log rather than emit them in
	// parallel with it, so a killed process loses nothing. That belongs with the Milestone 5
	// event-log work, and until then this gap is real and should be stated rather than papered
	// over: the viewer can be up to a second behind the database, and a hard kill can leave the
	// last second missing from the trace while Postgres has it.
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(time.Second)),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(provider)
	log.Info("tracing enabled", "endpoint", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	return provider.Shutdown, nil
}

// RunContext returns a context whose parent span is derived from the run id rather than generated.
//
// This is what makes the kill-and-resume story legible in a trace viewer. A run that dies and is
// picked up by a different process would otherwise produce two unrelated traces, because a fresh
// root span gets a random trace id. Deriving the trace id from the run id instead means every
// attempt at a run — however many processes it takes — lands in one continuous trace, with the
// replayed steps sitting next to the ones that executed for real.
func RunContext(ctx context.Context, runID string) context.Context {
	sum := sha256.Sum256([]byte(runID))
	var traceID oteltrace.TraceID
	copy(traceID[:], sum[:16])
	var spanID oteltrace.SpanID
	copy(spanID[:], sum[16:24])

	return oteltrace.ContextWithSpanContext(ctx, oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: oteltrace.FlagsSampled,
		Remote:     true,
	}))
}

// Span is the handle returned by Start. Aliased so instrumented packages import one tracing
// package rather than three OpenTelemetry ones.
type Span = oteltrace.Span

// Start begins a span carrying the given attributes.
func Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, Span) {
	return Tracer().Start(ctx, name, oteltrace.WithAttributes(attrs...))
}

// Fail marks a span as failed. A nil error is ignored, so callers can defer it unconditionally
// against a named return.
func Fail(span Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// The attribute constructors the callers need, re-exported for the same reason as Span.
var (
	String = attribute.String
	Int    = attribute.Int
	Bool   = attribute.Bool
)
