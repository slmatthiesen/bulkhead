// Command bulkheadd is the bulkhead control plane: it accepts runs, executes them one at a time,
// checkpoints every step to Postgres, and picks up whatever was in flight when it last died.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/slmatthiesen/bulkhead/internal/executor"
	"github.com/slmatthiesen/bulkhead/internal/run"
	"github.com/slmatthiesen/bulkhead/internal/store"
	"github.com/slmatthiesen/bulkhead/internal/trace"
	"github.com/slmatthiesen/bulkhead/internal/web"
	"github.com/slmatthiesen/bulkhead/internal/workload/supplychain"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := serve(log); err != nil {
		log.Error("control plane exited", "err", err)
		os.Exit(1)
	}
}

func serve(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := trace.Setup(ctx, log)
	if err != nil {
		return err
	}
	// Spans are batched, so an unflushed exit loses the tail of the trajectory — the part that
	// matters most, since it is what was in flight when the process went down. This gets its own
	// context because ctx is already cancelled by the time the deferred call runs.
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(flushCtx); err != nil {
			log.Error("could not flush traces", "err", err)
		}
	}()

	db, err := store.Open(ctx, envOr("BULKHEAD_DATABASE_URL",
		"postgres://bulkhead:bulkhead@127.0.0.1:5432/bulkhead?sslmode=disable"))
	if err != nil {
		return err
	}
	defer db.Close()

	demo, err := supplychain.Open(ctx, envOr("BULKHEAD_DATABASE_URL",
		"postgres://bulkhead:bulkhead@127.0.0.1:5432/bulkhead?sslmode=disable"))
	if err != nil {
		return err
	}
	defer demo.Close()
	if delay, err := time.ParseDuration(envOr("BULKHEAD_DEMO_STEP_DELAY", "0s")); err != nil {
		return fmt.Errorf("BULKHEAD_DEMO_STEP_DELAY: %w", err)
	} else {
		demo.StepDelay = delay
	}

	exec, err := newExecutor(log, demo)
	if err != nil {
		return err
	}
	defer exec.Close()

	agentCmd := strings.Fields(envOr("BULKHEAD_AGENT_CMD", "uv run bulkhead-agent"))
	if len(agentCmd) == 0 {
		return errors.New("BULKHEAD_AGENT_CMD is empty")
	}
	engine := run.NewEngine(db, exec, agentCmd, log)

	// Milestone 1 executes runs serially in this process. The queue is a channel; Milestone 3
	// replaces it with Redis Streams and stateless workers, which is why nothing outside this
	// file knows how work is dispatched.
	queue := make(chan string, 1024)
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		work(ctx, db, engine, queue, log)
	}()

	// Anything still marked queued or running belongs to a control plane that died. Nothing else
	// can leave a run in that state, so recovering them at startup is unambiguous.
	resumed, err := db.UnfinishedRuns(ctx)
	if err != nil {
		return err
	}
	for _, r := range resumed {
		log.Info("resuming interrupted run", "run", r.ID, "state", r.State, "completed_steps", r.StepCount)
		queue <- r.ID
	}

	addr := envOr("BULKHEAD_ADDR", "127.0.0.1:8080")
	srv := &http.Server{
		Addr:              addr,
		Handler:           routes(ctx, db, demo, queue, log),
		ReadHeaderTimeout: 5 * time.Second,
	}
	listenErr := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", addr, "resumed_runs", len(resumed))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			listenErr <- err
		}
	}()

	select {
	case err := <-listenErr:
		return err
	case <-ctx.Done():
	}

	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdownCtx)
	workers.Wait()
	return err
}

func work(ctx context.Context, db *store.Store, engine *run.Engine, queue <-chan string, log *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-queue:
			r, err := db.GetRun(ctx, id)
			if err != nil {
				log.Error("cannot load queued run", "run", id, "err", err)
				continue
			}
			if r.State == store.StateSucceeded || r.State == store.StateFailed {
				continue
			}
			if err := engine.Execute(ctx, r); err != nil && ctx.Err() == nil {
				log.Error("run failed", "run", id, "err", err)
			}
		}
	}
}

// routes takes the process context so that open event streams end when the control plane is
// shutting down; without it a stream would hold the graceful shutdown open until it timed out.
func routes(ctx context.Context, db *store.Store, demo *supplychain.Store, queue chan<- string, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()

	mux.Handle("GET /", web.Handler())

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})

	mux.HandleFunc("POST /runs", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			InstanceID string `json:"instance_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
		if body.InstanceID == "" {
			http.Error(w, "instance_id is required", http.StatusBadRequest)
			return
		}
		created, err := db.CreateRun(r.Context(), body.InstanceID)
		if err != nil {
			log.Error("create run", "err", err)
			http.Error(w, "could not create run", http.StatusInternalServerError)
			return
		}
		queue <- created.ID
		w.WriteHeader(http.StatusCreated)
		writeJSON(w, map[string]string{"id": created.ID})
	})

	mux.HandleFunc("GET /runs/{id}", func(w http.ResponseWriter, r *http.Request) {
		found, err := db.GetRun(r.Context(), r.PathValue("id"))
		if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "no such run", http.StatusNotFound)
			return
		}
		if err != nil {
			log.Error("get run", "err", err)
			http.Error(w, "could not load run", http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]any{
			"id":           found.ID,
			"instance_id":  found.InstanceID,
			"state":        found.State,
			"step_count":   found.StepCount,
			"patch":        found.Patch,
			"tests_passed": found.TestsPassed,
			"error":        found.Error,
		})
	})

	// The trajectory is tailed by polling Postgres, because Postgres is where the durable record
	// already is. Milestone 3 brings Redis and turns this into a subscription; a reader that
	// reconnects still resumes from its last event id either way.
	mux.HandleFunc("GET /runs/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		id := r.PathValue("id")
		if _, err := db.GetRun(r.Context(), id); errors.Is(err, store.ErrNotFound) {
			http.Error(w, "no such run", http.StatusNotFound)
			return
		}

		after := lastEventID(r)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		flusher.Flush()

		poll := time.NewTicker(200 * time.Millisecond)
		defer poll.Stop()
		for {
			events, err := db.EventsSince(r.Context(), id, after)
			if err != nil {
				if r.Context().Err() == nil {
					log.Error("tail trajectory", "run", id, "err", err)
				}
				return
			}
			for _, e := range events {
				payload, err := json.Marshal(e)
				if err != nil {
					log.Error("encode trajectory event", "run", id, "event", e.ID, "err", err)
					return
				}
				// The event id is what the browser sends back as Last-Event-ID on reconnect.
				fmt.Fprintf(w, "id: %d\ndata: %s\n\n", e.ID, payload)
				after = e.ID
			}
			flusher.Flush()

			select {
			case <-r.Context().Done():
				return
			case <-ctx.Done():
				return
			case <-poll.C:
			}
		}
	})

	// The kill button. os.Exit skips every deferred close, flush and shutdown, which is the point:
	// a demo of crash recovery that shut down gracefully would be demonstrating nothing. The reply
	// is flushed first so the browser learns the request succeeded before the process disappears.
	mux.HandleFunc("POST /demo/kill", func(w http.ResponseWriter, _ *http.Request) {
		log.Warn("kill requested from the demo console")
		writeJSON(w, map[string]string{"status": "killing"})
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		go func() {
			time.Sleep(150 * time.Millisecond)
			os.Exit(137) // what a SIGKILL looks like from the outside
		}()
	})

	mux.HandleFunc("POST /demo/reseed", func(w http.ResponseWriter, r *http.Request) {
		if err := demo.Reseed(r.Context()); err != nil {
			log.Error("reseed", "err", err)
			http.Error(w, "could not reseed", http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]string{"status": "reseeded"})
	})

	mux.HandleFunc("GET /demo/inventory", func(w http.ResponseWriter, r *http.Request) {
		rows, err := demo.Inventory(r.Context())
		if err != nil {
			log.Error("inventory", "err", err)
			http.Error(w, "could not read inventory", http.StatusInternalServerError)
			return
		}
		writeJSON(w, rows)
	})

	mux.HandleFunc("GET /demo/orders", func(w http.ResponseWriter, r *http.Request) {
		orders, err := demo.Orders(r.Context())
		if err != nil {
			log.Error("orders", "err", err)
			http.Error(w, "could not read orders", http.StatusInternalServerError)
			return
		}
		writeJSON(w, orders)
	})

	// The under-the-hood view: the checkpoint rows the replay actually reads from.
	mux.HandleFunc("GET /runs/{id}/steps", func(w http.ResponseWriter, r *http.Request) {
		steps, err := db.Steps(r.Context(), r.PathValue("id"))
		if err != nil {
			log.Error("steps", "err", err)
			http.Error(w, "could not read steps", http.StatusInternalServerError)
			return
		}
		writeJSON(w, steps)
	})

	return mux
}

// lastEventID reports where a reconnecting reader left off: the SSE header the browser sets
// automatically, or an explicit ?after= for anything that is not an EventSource.
func lastEventID(r *http.Request) int64 {
	for _, raw := range []string{r.Header.Get("Last-Event-ID"), r.URL.Query().Get("after")} {
		if id, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return id
		}
	}
	return 0
}

// newExecutor assembles the executor for each step kind the agent can ask for.
//
// Model steps are registered only when there is a credential to make them with. Leaving the kind
// unregistered rather than constructing a client that will fail on first use keeps the acceptance
// test exactly as it was — it drives a deterministic fixture agent that asks for commands only,
// needs no API key, and costs nothing — while a run that asks for a model step without one fails
// with a message naming the kinds that do exist.
func newExecutor(log *slog.Logger, demo *supplychain.Store) (executor.Executor, error) {
	byKind := map[string]executor.Executor{}

	switch kind := envOr("BULKHEAD_EXECUTOR", "fixture"); kind {
	case "fixture":
		delay, err := time.ParseDuration(envOr("BULKHEAD_FIXTURE_STEP_DELAY", "0s"))
		if err != nil {
			return nil, fmt.Errorf("BULKHEAD_FIXTURE_STEP_DELAY: %w", err)
		}
		byKind[executor.KindCommand] = executor.NewFixture(os.Getenv("BULKHEAD_FIXTURE_LEDGER"), delay)
	default:
		return nil, fmt.Errorf("unknown BULKHEAD_EXECUTOR %q", kind)
	}

	byKind[supplychain.KindQuery] = demo.QueryExecutor()
	byKind[supplychain.KindEmail] = demo.EmailExecutor()

	// BULKHEAD_LLM_API_KEY wins over ANTHROPIC_API_KEY so that pointing the harness at cheap
	// inference for development does not mean unsetting a key the rest of the machine relies on.
	apiKey := envOr("BULKHEAD_LLM_API_KEY", os.Getenv("ANTHROPIC_API_KEY"))
	if apiKey == "" {
		log.Info("model steps unavailable", "reason", "no BULKHEAD_LLM_API_KEY or ANTHROPIC_API_KEY")
	} else {
		maxTokens, err := strconv.ParseInt(envOr("BULKHEAD_LLM_MAX_TOKENS", "16000"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("BULKHEAD_LLM_MAX_TOKENS: %w", err)
		}
		model := envOr("BULKHEAD_LLM_MODEL", "claude-opus-5")
		llm, err := executor.NewLLM(
			os.Getenv("BULKHEAD_LLM_PROVIDER"),
			os.Getenv("BULKHEAD_LLM_BASE_URL"),
			apiKey, model, maxTokens,
		)
		if err != nil {
			return nil, err
		}
		byKind[executor.KindLLM] = llm
		log.Info("model steps enabled", "provider", llm.Provider(), "model", model, "max_tokens", maxTokens)
	}

	return executor.NewRouter(byKind), nil
}

func writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(payload)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
