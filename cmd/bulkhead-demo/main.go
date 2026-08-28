// Command bulkhead-demo supervises the control plane and does not restart it on its own.
//
// A crashed process cannot bring itself back, so something has to, and in production that is a
// container runtime. For a demo the automatic version is wrong: the interesting moment is the
// window where the process is gone and the work is not, and a supervisor that restarts two seconds
// later closes that window before anyone can talk about it.
//
// So this waits. It serves a small control API on its own port -- deliberately a different one, so
// it is still answering when the control plane is not -- and starts the child only when asked. The
// console polls it to know whether it is looking at a crash it can recover from.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type supervisor struct {
	bin  string
	args []string

	mu      sync.Mutex
	cmd     *exec.Cmd
	running bool
	starts  int
	lastExt string
}

func main() {
	addr := envOr("BULKHEAD_SUPERVISOR_ADDR", "127.0.0.1:8081")
	s := &supervisor{bin: envOr("BULKHEAD_DAEMON_BIN", "./bin/bulkheadd.exe")}

	if err := s.start(); err != nil {
		log.Fatalf("supervisor: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		writeJSON(w, map[string]any{
			"running": s.running,
			"starts":  s.starts,
			"exit":    s.lastExt,
		})
	})
	mux.HandleFunc("POST /start", func(w http.ResponseWriter, r *http.Request) {
		if err := s.start(); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		writeJSON(w, map[string]string{"status": "starting"})
	})
	// Preflight, because the console is served by the control plane on a different port and calls
	// this one precisely when that port is dead.
	mux.HandleFunc("OPTIONS /", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })

	srv := &http.Server{Addr: addr, Handler: cors(mux), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("supervisor: control API on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("supervisor: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	s.mu.Lock()
	if s.running && s.cmd != nil && s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
	}
	s.mu.Unlock()
	log.Println("supervisor: stopped")
}

func (s *supervisor) start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return fmt.Errorf("control plane is already running")
	}

	cmd := exec.Command(s.bin, s.args...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start control plane: %w", err)
	}
	s.cmd, s.running, s.starts = cmd, true, s.starts+1
	s.lastExt = ""
	log.Printf("supervisor: control plane started (pid %d, start #%d)", cmd.Process.Pid, s.starts)

	go func() {
		err := cmd.Wait()
		s.mu.Lock()
		s.running = false
		s.lastExt = "exited cleanly"
		if err != nil {
			s.lastExt = err.Error()
		}
		s.mu.Unlock()
		log.Printf("supervisor: control plane %s -- waiting for a restart request", s.lastExt)
	}()
	return nil
}

func cors(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func envOr(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
