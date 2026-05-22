"""
Synthetic SFT Data Generator with BOS/EOS Masking
Source: karpathy/nanochat (Issue #570 — no masking for response loss degrades SFT)
         karpathy/llm.c (metal-level training pipeline)

THE PROBLEM (from nanochat Issue #570):
Running standard next-token prediction loss over the entire conversation — including
the prompt tokens — severely degrades SFT performance. The optimizer wastes gradient
signal trying to predict the human turn. Only the assistant response tokens should
contribute to the loss function.

THIS TOOL:
1. Uses the Anthropic API to generate deep, multi-turn technical dialogues
2. Formats output as JSONL using ChatML token format (nanochat-compatible)
3. Embeds `loss_mask_roles: ["assistant"]` metadata so data loaders know
   which turns to compute gradients on
4. Validates BOS/EOS structure before writing to disk

Output format (one JSON object per line):
{
  "messages": [
    {"role": "system", "content": "..."},
    {"role": "user", "content": "..."},
    {"role": "assistant", "content": "..."},
    {"role": "user", "content": "..."},
    {"role": "assistant", "content": "..."}
  ],
  "loss_mask_roles": ["assistant"],
  "chatml": "<full ChatML formatted string with BOS/EOS tokens>",
  "token_format": "chatml"
}

Usage:
    export ANTHROPIC_API_KEY=sk-...
    python generator.py --topic "Python decorators" --count 5 --output sft_data.jsonl

Requires: pip install anthropic
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, asdict
from pathlib import Path

import anthropic

MODEL = "claude-haiku-4-5"  # Fast + cheap for bulk data generation

# ChatML token format (nanochat / llm.c compatible)
BOS = "<|im_start|>"
EOS = "<|im_end|>"

GENERATION_SYSTEM = """You generate synthetic multi-turn technical dialogues for AI training datasets.
Every dialogue must:
1. Be technically accurate and deep — no surface-level explanations
2. Have the user ask progressively harder follow-up questions (3-4 turns minimum)
3. Have the assistant give precise, code-inclusive answers
4. Stay tightly focused on the topic provided

Output the dialogue as a JSON object with this exact structure:
{
  "system": "You are a helpful technical assistant.",
  "turns": [
    {"role": "user", "content": "..."},
    {"role": "assistant", "content": "..."},
    {"role": "user", "content": "..."},
    {"role": "assistant", "content": "..."}
  ]
}
Output valid JSON only. No markdown fences. No prose before or after."""

DEFAULT_TOPICS = [
    "Python context managers and the __enter__/__exit__ protocol",
    "How gradient descent converges in non-convex loss landscapes",
    "The difference between process and thread scheduling in Linux",
    "Why database transactions use MVCC instead of simple locking",
    "How TCP slow start and congestion avoidance work together",
]


@dataclass
class SFTRecord:
    messages: list[dict]
    loss_mask_roles: list[str]
    chatml: str
    token_format: str = "chatml"

    def to_jsonl_line(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)


def _build_chatml(messages: list[dict]) -> str:
    """
    Format messages as a ChatML string with BOS/EOS tokens.

    Structure:
        <|im_start|>role\\ncontent<|im_end|>\\n

    The data loader uses BOS/EOS markers to identify turn boundaries
    and applies loss_mask=0 to all non-assistant tokens.
    """
    parts = []
    for msg in messages:
        parts.append(f"{BOS}{msg['role']}\n{msg['content']}{EOS}\n")
    return "".join(parts)


def _validate_record(record: SFTRecord) -> list[str]:
    """Return list of validation errors. Empty list = valid."""
    errors = []
    if not record.messages:
        errors.append("messages list is empty")
    roles = {m["role"] for m in record.messages}
    if "assistant" not in roles:
        errors.append("no assistant turn found — loss mask would be all zeros")
    if not record.chatml.count(BOS) == len(record.messages):
        errors.append(f"BOS count mismatch: expected {len(record.messages)}, got {record.chatml.count(BOS)}")
    if not record.chatml.count(EOS) == len(record.messages):
        errors.append(f"EOS count mismatch: expected {len(record.messages)}, got {record.chatml.count(EOS)}")
    return errors


def generate_dialogue(topic: str, client: anthropic.Anthropic) -> SFTRecord | None:
    """
    Generate one multi-turn dialogue on the given topic.
    Returns SFTRecord on success, None on parse/validation failure.
    """
    prompt = f"Generate a deep technical dialogue about: {topic}"
    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=2048,
            system=GENERATION_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        data = json.loads(raw)

        system_content = data.get("system", "You are a helpful technical assistant.")
        turns = data["turns"]

        messages = [{"role": "system", "content": system_content}] + turns
        chatml = _build_chatml(messages)

        record = SFTRecord(
            messages=messages,
            loss_mask_roles=["assistant"],
            chatml=chatml,
        )

        errors = _validate_record(record)
        if errors:
            print(f"[WARN] Validation failed for topic '{topic}': {errors}", file=sys.stderr)
            return None

        return record

    except (json.JSONDecodeError, KeyError) as exc:
        print(f"[WARN] Parse error for topic '{topic}': {exc}", file=sys.stderr)
        return None


def generate_dataset(
    topics: list[str],
    output_path: Path,
    count_per_topic: int = 1,
) -> dict:
    """
    Generate `count_per_topic` dialogues per topic and write to JSONL.

    Returns summary stats: {"generated": N, "failed": M, "output_path": str}
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError("ANTHROPIC_API_KEY environment variable is not set.")

    client = anthropic.Anthropic(api_key=api_key)
    generated = 0
    failed = 0

    with output_path.open("w", encoding="utf-8") as f:
        for topic in topics:
            for _ in range(count_per_topic):
                print(f"[GEN] Generating: {topic[:60]}...")
                record = generate_dialogue(topic, client)
                if record:
                    f.write(record.to_jsonl_line() + "\n")
                    generated += 1
                    print(f"[GEN] OK — {len(record.messages)} turns, {len(record.chatml)} chars")
                else:
                    failed += 1
                    print(f"[GEN] FAILED — {topic[:60]}")

    return {"generated": generated, "failed": failed, "output_path": str(output_path)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate SFT training data with BOS/EOS masking")
    parser.add_argument("--topic", type=str, default=None, help="Single topic to generate")
    parser.add_argument("--count", type=int, default=1, help="Dialogues per topic")
    parser.add_argument("--output", type=str, default="sft_data.jsonl", help="Output JSONL path")
    args = parser.parse_args()

    topics = [args.topic] if args.topic else DEFAULT_TOPICS
    output = Path(args.output)

    stats = generate_dataset(topics, output, count_per_topic=args.count)
    print(f"\n[DONE] Generated: {stats['generated']}  Failed: {stats['failed']}")
    print(f"[DONE] Output: {stats['output_path']}")
