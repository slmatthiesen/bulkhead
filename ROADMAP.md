# Roadmap

Each milestone states how it is verified. A milestone is not done until its `verify` passes.

### 1. Single-node harness — *in progress*
One isolation tier (plain container), 10 SWE-bench Verified instances end to end, run serially.

> **verify:** checkpoint a run mid-flight, `kill -9` the control plane, resume, identical output.

### 2. Four isolation tiers behind one interface
Plain container, gVisor, Firecracker microVM, WASM/WASI.

> **verify:** benchmark table published with real numbers on an identical workload.

### 3. Queue + warm pool + stateless workers

> **verify:** throughput scales roughly linearly to N workers, and the bottleneck can be named.

### 4. k3s with KEDA queue-depth autoscaling

> **verify:** pods scale up under load, drain cleanly, zero dropped work on pod kill.

### 5. Redis Streams → Kafka migration for dispatch

> **verify:** before/after throughput numbers plus a written rationale with the crossover point.

### 6. Failure injection
Kill a sandbox mid-execution, hang a test suite, forkbomb, blow a provider rate limit, poison an
instance.

> **verify:** no duplicate side effects, no stuck work, documented recovery.

### 7. Second workload
Document extraction behind the same pluggable interface.

> **verify:** the new workload is added without touching the control plane.

### 8. Human-in-the-loop approval gate *(optional)*

> **verify:** pause a run for approval, approve or reject, resume.

## Scope discipline

Milestone 6 is what separates this from a demo. If scope has to be cut, cut a sandbox tier or the
second workload — never the Go control plane, and never the failure-injection write-up.
