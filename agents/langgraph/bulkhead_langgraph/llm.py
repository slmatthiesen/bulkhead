"""The one model call in the workflow: a two-sentence summary for the purchasing team.

Claude via the Bedrock Converse API through LangChain, so the same code runs on a laptop with AWS
credentials and inside AgentCore Runtime with its execution role. Token counts come back with the
text so the run can be costed.
"""

from __future__ import annotations

import os

from langchain_aws import ChatBedrockConverse

DEFAULT_MODEL = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"

SYSTEM = ("You are a warehouse operations assistant. Answer the question in two sentences at most, "
          "no preamble. Ground every claim in the stock figures you are given.")


def stock_lines(stock: list[dict]) -> str:
    lines = []
    for r in stock:
        line = f"{r['name']}: {r['on_hand']} on hand, reorder below {r['reorder_threshold']}"
        if r["daily_usage"]:
            line += f", using {r['daily_usage']}/day ({r['on_hand'] / r['daily_usage']:.1f} days left)"
        lines.append(line)
    return "\n".join(lines)


def summarise(stock: list[dict], question: str) -> dict:
    model = ChatBedrockConverse(
        model=os.environ.get("BULKHEAD_MODEL", DEFAULT_MODEL),
        region_name=os.environ.get("AWS_REGION", "us-west-2"),
        max_tokens=400,
        temperature=0,
    )
    ask = question.strip() or "Summarise the situation for the purchasing team."
    msg = model.invoke([
        ("system", SYSTEM),
        ("human", "Current stock below reorder threshold:\n" + stock_lines(stock) + "\n\n" + ask),
    ])
    usage = msg.usage_metadata or {}
    return {
        "text": msg.content if isinstance(msg.content, str) else "".join(p.get("text", "") for p in msg.content),
        "input_tokens": usage.get("input_tokens", 0),
        "output_tokens": usage.get("output_tokens", 0),
        "model": model.model_id,
    }
