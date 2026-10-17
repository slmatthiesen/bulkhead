"""AgentCore Runtime entrypoint. The same graph the CLI runs, behind the Runtime's HTTP contract.

Payload: {"thread_id": "...", "resume": "yes" | null, "question": "...", "reseed": false}
thread_id defaults to the Runtime session id, so re-invoking the same session after a crash is
what resumes the run.
"""

from bedrock_agentcore import BedrockAgentCoreApp

from bulkhead_langgraph.runner import run_thread

app = BedrockAgentCoreApp()


@app.entrypoint
def invoke(payload: dict, context=None) -> dict:
    thread_id = payload.get("thread_id") or getattr(context, "session_id", None) or "default"
    return run_thread(
        thread_id,
        resume=payload.get("resume"),
        question=payload.get("question", ""),
        reseed=bool(payload.get("reseed", False)),
    )


if __name__ == "__main__":
    app.run()
