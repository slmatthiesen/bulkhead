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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/slmatthiesen/bulkhead/internal/executor"
	"github.com/slmatthiesen/bulkhead/internal/run"
	"github.com/slmatthiesen/bulkhead/internal/store"
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

	db, err := store.Open(ctx, envOr("BULKHEAD_DATABASE_URL",
		"postgres://bulkhead:bulkhead@127.0.0.1:5432/bulkhead?sslmode=disable"))
	if err != nil {
		return err
	}
	defer db.Close()

	exec, err := newExecutor()
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
		Handler:           routes(db, queue, log),
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

func routes(db *store.Store, queue chan<- string, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()

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

	return mux
}

func newExecutor() (executor.Executor, error) {
	switch kind := envOr("BULKHEAD_EXECUTOR", "fixture"); kind {
	case "fixture":
		delay, err := time.ParseDuration(envOr("BULKHEAD_FIXTURE_STEP_DELAY", "0s"))
		if err != nil {
			return nil, fmt.Errorf("BULKHEAD_FIXTURE_STEP_DELAY: %w", err)
		}
		return executor.NewFixture(os.Getenv("BULKHEAD_FIXTURE_LEDGER"), delay), nil
	default:
		return nil, fmt.Errorf("unknown BULKHEAD_EXECUTOR %q", kind)
	}
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
