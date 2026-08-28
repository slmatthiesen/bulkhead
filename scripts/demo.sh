#!/usr/bin/env bash
# Runs the demo: a supervisor that starts the control plane and then waits.
#
# It does not restart on its own. The kill button in the console ends the process, and the console
# then offers a Restart button -- because the window where the process is gone and the work is not
# is the whole point, and a supervisor that closes it after two seconds closes it before anyone can
# talk about it.
#
# The supervisor's control API is on its own port so that it is still answering when the control
# plane is not.
set -uo pipefail
cd "$(dirname "$0")/.."

[ -f .env.local ] && { set -a; . ./.env.local; set +a; }

export BULKHEAD_AGENT_CMD="${BULKHEAD_AGENT_CMD:-$PWD/bin/supplychain-agent.exe}"
export BULKHEAD_AGENT_USE_LLM="${BULKHEAD_AGENT_USE_LLM:-1}"
export BULKHEAD_DEMO_STEP_DELAY="${BULKHEAD_DEMO_STEP_DELAY:-4s}"
export BULKHEAD_ADDR="${BULKHEAD_ADDR:-127.0.0.1:8080}"
export BULKHEAD_SUPERVISOR_ADDR="${BULKHEAD_SUPERVISOR_ADDR:-127.0.0.1:8081}"
export BULKHEAD_DAEMON_BIN="${BULKHEAD_DAEMON_BIN:-$PWD/bin/bulkheadd.exe}"

exec ./bin/bulkhead-demo.exe
