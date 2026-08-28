#!/usr/bin/env bash
# Runs the demo control plane under a supervisor.
#
# The kill button in the console calls os.Exit, which is the point -- crash recovery demonstrated
# by a graceful shutdown is not a demonstration. But a dead process cannot restart itself, so
# something has to. In production that is a container runtime restarting the pod; here it is this
# loop, and the behaviour is the same: the process comes back, finds the run that was in flight,
# and resumes it from its checkpoints without anyone asking.
set -uo pipefail
cd "$(dirname "$0")/.."

[ -f .env.local ] && { set -a; . ./.env.local; set +a; }

export BULKHEAD_AGENT_CMD="${BULKHEAD_AGENT_CMD:-$PWD/bin/supplychain-agent.exe}"
export BULKHEAD_AGENT_USE_LLM="${BULKHEAD_AGENT_USE_LLM:-1}"
export BULKHEAD_DEMO_STEP_DELAY="${BULKHEAD_DEMO_STEP_DELAY:-4s}"
export BULKHEAD_ADDR="${BULKHEAD_ADDR:-127.0.0.1:8080}"

echo "supervisor: starting control plane on $BULKHEAD_ADDR"
while true; do
    ./bin/bulkheadd.exe
    code=$?
    # Three seconds, not one. The restart is not the interesting part -- the outage is, and a
    # blink too short for the console to notice is a blink too short for an audience to notice.
    echo "supervisor: control plane exited ($code), restarting in 2s"
    sleep 2
done
