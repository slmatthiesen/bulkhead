//go:build acceptance

// Package acceptance holds the single test that defines Milestone 1 as done.
//
// Run it with:
//
//	docker compose up -d
//	go test -tags acceptance ./test/acceptance/... -v
//
// It is a black-box test on purpose: it builds the real binaries, starts the real control plane as
// a child process, talks to it over HTTP, and kills it with an uncatchable signal. Nothing is
// stubbed inside the process under test, because the failure mode being tested — losing work
// because the process died between "side effect happened" and "we wrote that down" — only exists
// at that boundary.
package acceptance

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	// The fixture agent always asks for the same steps in the same order, so a difference between
	// two runs can only come from the control plane. See cmd/fixture-agent.
	fixtureInstance = "django__django-11099"

	// Each executed step sleeps this long, which gives the test a wide window in which to kill the
	// control plane mid-run rather than racing a run that finishes in microseconds.
	fixtureStepDelay = "250ms"

	terminalTimeout = 90 * time.Second
	startupTimeout  = 20 * time.Second
)

var (
	daemonBin  string
	fixtureBin string
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "bulkhead-acceptance-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "mktemp:", err)
		os.Exit(1)
	}
	daemonBin, err = build(dir, "bulkheadd")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fixtureBin, err = build(dir, "fixture-agent")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// TestKilledRunResumesToIdenticalResult is the Milestone 1 acceptance criterion.
//
// Start a run, let it get past its first tool call, kill -9 the control plane, restart it. The run
// must resume from its last checkpoint, finish with exactly the result an uninterrupted run
// produces, and must not execute any step a second time.
func TestKilledRunResumesToIdenticalResult(t *testing.T) {
	baselineLedger := filepath.Join(t.TempDir(), "baseline.ledger")
	interruptedLedger := filepath.Join(t.TempDir(), "interrupted.ledger")

	// A clean run of the same instance, for comparison. Anything the interrupted run does
	// differently is a checkpointing bug.
	baseline := func() runView {
		d := startDaemon(t, baselineLedger)
		defer d.stop()
		id := d.startRun(t, fixtureInstance)
		return d.awaitTerminal(t, id)
	}()
	if baseline.State != "succeeded" {
		t.Fatalf("baseline run did not succeed: state=%q error=%q", baseline.State, baseline.Error)
	}

	// The same instance again, interrupted.
	first := startDaemon(t, interruptedLedger)
	runID := first.startRun(t, fixtureInstance)
	first.awaitSteps(t, runID, 2) // past the first tool call, as the criterion requires
	first.kill(t)

	second := startDaemon(t, interruptedLedger)
	defer second.stop()
	resumed := second.awaitTerminal(t, runID)

	if resumed.State != baseline.State {
		t.Errorf("state after resume = %q, want %q (error: %q)", resumed.State, baseline.State, resumed.Error)
	}
	if resumed.Patch != baseline.Patch {
		t.Errorf("patch after resume differs from baseline:\n--- resumed ---\n%s\n--- baseline ---\n%s",
			resumed.Patch, baseline.Patch)
	}
	if resumed.TestsPassed != baseline.TestsPassed {
		t.Errorf("tests_passed after resume = %v, want %v", resumed.TestsPassed, baseline.TestsPassed)
	}
	if resumed.StepCount != baseline.StepCount {
		t.Errorf("step count after resume = %d, want %d", resumed.StepCount, baseline.StepCount)
	}

	// The result matching is not enough on its own: a control plane that re-ran every step from
	// scratch would also produce a matching patch, while having executed every side effect twice.
	// The ledger is appended to once per actual execution, so it is where that shows up.
	executed := readLedger(t, interruptedLedger)
	if dup := duplicates(executed); len(dup) > 0 {
		t.Errorf("steps executed more than once across the kill: %v", dup)
	}
	if len(executed) != baseline.StepCount {
		t.Errorf("executed %d steps across the interrupted run, want %d — resume should replay "+
			"checkpointed steps, not re-execute them", len(executed), baseline.StepCount)
	}
}

// runView mirrors the control plane's GET /runs/{id} response.
type runView struct {
	ID          string `json:"id"`
	InstanceID  string `json:"instance_id"`
	State       string `json:"state"`
	StepCount   int    `json:"step_count"`
	Patch       string `json:"patch"`
	TestsPassed bool   `json:"tests_passed"`
	Error       string `json:"error"`
}

func (r runView) terminal() bool {
	return r.State == "succeeded" || r.State == "failed"
}

type daemon struct {
	cmd    *exec.Cmd
	addr   string
	output *syncBuffer
	dead   bool
}

// startDaemon boots a control plane against the shared Postgres, pointed at the fixture agent and
// the fixture executor. Successive daemons in one test share a ledger so that "executed twice"
// spans the restart.
func startDaemon(t *testing.T, ledger string) *daemon {
	t.Helper()

	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	cmd := exec.Command(daemonBin)
	cmd.Env = append(os.Environ(),
		"BULKHEAD_ADDR="+addr,
		"BULKHEAD_DATABASE_URL="+databaseURL(),
		"BULKHEAD_AGENT_CMD="+fixtureBin,
		"BULKHEAD_EXECUTOR=fixture",
		"BULKHEAD_FIXTURE_LEDGER="+ledger,
		"BULKHEAD_FIXTURE_STEP_DELAY="+fixtureStepDelay,
	)
	out := &syncBuffer{}
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start control plane: %v", err)
	}

	d := &daemon{cmd: cmd, addr: addr, output: out}
	t.Cleanup(func() {
		d.stop()
		if t.Failed() {
			t.Logf("control plane on %s said:\n%s", addr, out.String())
		}
	})
	d.awaitHealthy(t)
	return d
}

func (d *daemon) awaitHealthy(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(startupTimeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(d.url("/healthz"))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if d.cmd.ProcessState != nil {
			t.Fatalf("control plane exited during startup:\n%s", d.output.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("control plane never became healthy on %s:\n%s", d.addr, d.output.String())
}

func (d *daemon) url(path string) string { return "http://" + d.addr + path }

func (d *daemon) startRun(t *testing.T, instance string) string {
	t.Helper()
	body := strings.NewReader(fmt.Sprintf(`{"instance_id":%q}`, instance))
	resp, err := http.Post(d.url("/runs"), "application/json", body)
	if err != nil {
		t.Fatalf("POST /runs: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		payload, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST /runs = %d %s, want 201", resp.StatusCode, strings.TrimSpace(string(payload)))
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode POST /runs response: %v", err)
	}
	if created.ID == "" {
		t.Fatal("POST /runs returned an empty run id")
	}
	return created.ID
}

func (d *daemon) get(t *testing.T, runID string) runView {
	t.Helper()
	resp, err := http.Get(d.url("/runs/" + runID))
	if err != nil {
		t.Fatalf("GET /runs/%s: %v", runID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET /runs/%s = %d %s, want 200", runID, resp.StatusCode, strings.TrimSpace(string(payload)))
	}
	var view runView
	if err := json.NewDecoder(resp.Body).Decode(&view); err != nil {
		t.Fatalf("decode GET /runs/%s: %v", runID, err)
	}
	return view
}

// awaitSteps blocks until the run has checkpointed at least n steps, so the kill lands mid-run.
func (d *daemon) awaitSteps(t *testing.T, runID string, n int) {
	t.Helper()
	deadline := time.Now().Add(terminalTimeout)
	for time.Now().Before(deadline) {
		view := d.get(t, runID)
		if view.StepCount >= n {
			return
		}
		if view.terminal() {
			t.Fatalf("run reached %q after only %d steps; it must still be in flight when killed",
				view.State, view.StepCount)
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("run %s never reached %d steps", runID, n)
}

func (d *daemon) awaitTerminal(t *testing.T, runID string) runView {
	t.Helper()
	deadline := time.Now().Add(terminalTimeout)
	for time.Now().Before(deadline) {
		if view := d.get(t, runID); view.terminal() {
			return view
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("run %s never reached a terminal state within %s", runID, terminalTimeout)
	return runView{}
}

// kill terminates the control plane without giving it a chance to clean up — the whole point is
// that durability cannot depend on a shutdown hook running.
func (d *daemon) kill(t *testing.T) {
	t.Helper()
	if err := d.cmd.Process.Kill(); err != nil {
		t.Fatalf("kill control plane: %v", err)
	}
	d.cmd.Wait()
	d.dead = true
}

func (d *daemon) stop() {
	if d.dead || d.cmd.Process == nil {
		return
	}
	d.cmd.Process.Kill()
	d.cmd.Wait()
	d.dead = true
}

// readLedger returns the step keys the executor recorded, one per actual execution.
func readLedger(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open executor ledger: %v", err)
	}
	defer f.Close()

	var keys []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if line := strings.TrimSpace(scanner.Text()); line != "" {
			keys = append(keys, line)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read executor ledger: %v", err)
	}
	return keys
}

func duplicates(keys []string) []string {
	seen := make(map[string]int, len(keys))
	var dup []string
	for _, k := range keys {
		seen[k]++
		if seen[k] == 2 {
			dup = append(dup, k)
		}
	}
	return dup
}

func build(dir, name string) (string, error) {
	bin := filepath.Join(dir, name)
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/"+name)
	cmd.Dir = repoRoot()
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("build %s: %v\n%s", name, err, out)
	}
	return bin, nil
}

func repoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..")
}

func databaseURL() string {
	if v := os.Getenv("BULKHEAD_DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://bulkhead:bulkhead@127.0.0.1:5432/bulkhead?sslmode=disable"
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
