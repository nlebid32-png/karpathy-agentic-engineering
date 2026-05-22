"""
LLM Council Harness
Source: karpathy/llm-council; Muon orthogonalization concept from modded-nanogpt/llm.c

The problem this solves: post-training SFT homogenizes all Claude instances into
a deferential, balanced "assistant" persona. If you ask multiple instances to review
the same decision, their outputs are highly correlated — redundant, not additive.

Solution (from Muon optimizer): orthogonalize the agents. Assign disjoint,
non-overlapping constraints so no two agents can produce the same output.
Then a Chairman synthesizes the orthogonal vectors into a final verdict.

Protocol:
1. 5 isolated advisors with extreme, disjoint personas (parallel dispatch)
2. Anonymous circular peer review (each advisor reviews one other, blind)
3. Chairman synthesizes all analyses into a structured GO/NO-GO verdict

Usage:
    export ANTHROPIC_API_KEY=sk-...
    python council.py "Should we use PostgreSQL or MongoDB for this schema?"

    Or in code:
    from council import run_council
    verdict = run_council("your architectural question")
    print(verdict.chairman_synthesis)

Requires: pip install anthropic
"""
import json
import os
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass

import anthropic

MODEL = "claude-sonnet-4-6"

ADVISOR_PERSONAS: list[dict] = [
    {
        "name": "Contrarian",
        "system": (
            "You are a Contrarian technical advisor. You are strictly forbidden from "
            "being balanced, diplomatic, or polite. Your sole objective is to identify "
            "fatal flaws, edge-case failures, and catastrophic risks in the proposal. "
            "Every response must open with a concrete, specific fatal flaw. "
            "Do not hedge. Do not compliment anything."
        ),
    },
    {
        "name": "Expansionist",
        "system": (
            "You are an Expansionist technical advisor. Ignore all downside risks and "
            "failure modes entirely — they are not your concern. Your sole objective is "
            "to identify missing upstream potential, scale opportunities, and adjacent "
            "capabilities that are not being exploited. Be ambitious and specific."
        ),
    },
    {
        "name": "SecurityParanoid",
        "system": (
            "You are a Security Paranoid technical advisor. Assume all users are "
            "adversarial. Your sole objective is to enumerate every attack vector, "
            "data exposure, privilege escalation path, and trust boundary violation "
            "in the proposal. Do not discuss performance or features."
        ),
    },
    {
        "name": "PerformanceOptimizer",
        "system": (
            "You are a Performance Optimizer technical advisor. You care only about "
            "latency, throughput, memory efficiency, and computational cost. Your sole "
            "objective is to identify every bottleneck, unnecessary allocation, "
            "and scalability cliff. Do not discuss security or architecture."
        ),
    },
    {
        "name": "Minimalist",
        "system": (
            "You are a Minimalist technical advisor following Karpathy's principle: "
            "the simplest implementation that actually works beats a complex one that might. "
            "Your sole objective is to identify every unnecessary abstraction, premature "
            "optimization, and over-engineering in the proposal. "
            "Advocate ruthlessly for deletion and simplification."
        ),
    },
]

PEER_REVIEW_SYSTEM = (
    "You are reviewing a peer's technical analysis anonymously — you do not know who wrote it. "
    "Identify gaps, logical errors, and missed considerations in their analysis. "
    "Be specific. No flattery."
)

CHAIRMAN_SYSTEM = (
    "You are the Chairman of a technical review council. "
    "You have received 5 independent analyses and 5 peer reviews of a proposal. "
    "Synthesize these orthogonal perspectives into one actionable verdict.\n\n"
    "Structure your output exactly as follows:\n"
    "1. CRITICAL RISKS (must address before proceeding)\n"
    "2. KEY OPPORTUNITIES (worth pursuing)\n"
    "3. SECURITY FLAGS (must harden)\n"
    "4. PERFORMANCE CONCERNS (monitor or optimize)\n"
    "5. SIMPLIFICATION WINS (cut this)\n"
    "6. FINAL VERDICT: GO / NO-GO / CONDITIONAL-GO — one sentence rationale\n\n"
    "Be decisive. No flattery. No hedging."
)


@dataclass
class CouncilVerdict:
    query: str
    advisor_analyses: dict[str, str]
    peer_reviews: dict[str, str]
    chairman_synthesis: str

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)


def _anonymize_analyses(analyses: dict[str, str]) -> tuple[dict[str, str], dict[str, str]]:
    """
    Replace advisor names with opaque IDs before peer review and Chairman phases.
    Prevents the Chairman from weighting analyses based on known persona identities.

    Returns:
        anonymized: {opaque_id -> analysis_text}  (e.g. "Voice_A" -> text)
        reveal_map: {opaque_id -> original_name}  (kept by orchestrator only)
    """
    keys = list(analyses.keys())
    random.shuffle(keys)  # randomize ordering too
    anonymized: dict[str, str] = {}
    reveal_map: dict[str, str] = {}
    for i, key in enumerate(keys):
        opaque_id = f"Voice_{chr(65 + i)}"  # Voice_A, Voice_B, Voice_C, ...
        anonymized[opaque_id] = analyses[key]
        reveal_map[opaque_id] = key
    return anonymized, reveal_map


def _call_advisor(persona: dict, query: str, client: anthropic.Anthropic) -> tuple[str, str]:
    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=persona["system"],
        messages=[{"role": "user", "content": query}],
    )
    return persona["name"], response.content[0].text


def _call_peer_review(
    reviewer_name: str,
    reviewee_name: str,
    analysis_text: str,
    client: anthropic.Anthropic,
) -> tuple[str, str]:
    response = client.messages.create(
        model=MODEL,
        max_tokens=512,
        system=PEER_REVIEW_SYSTEM,
        messages=[{
            "role": "user",
            "content": f"Review this technical analysis:\n\n{analysis_text}",
        }],
    )
    key = f"{reviewer_name}_reviews_{reviewee_name}"
    return key, response.content[0].text


def _call_chairman(
    query: str,
    analyses: dict[str, str],
    reviews: dict[str, str],
    client: anthropic.Anthropic,
) -> str:
    brief = f"ORIGINAL QUERY:\n{query}\n\n"
    for name, analysis in analyses.items():
        brief += f"--- {name.upper()} ANALYSIS ---\n{analysis}\n\n"
    for key, review in reviews.items():
        brief += f"--- PEER REVIEW ({key}) ---\n{review}\n\n"

    response = client.messages.create(
        model=MODEL,
        max_tokens=2048,
        system=CHAIRMAN_SYSTEM,
        messages=[{"role": "user", "content": brief}],
    )
    return response.content[0].text


def run_council(query: str) -> CouncilVerdict:
    """
    Run the full LLM Council protocol on a decision query.

    Pass condition: returns a CouncilVerdict with all 5 advisor analyses,
    5 peer reviews, and chairman_synthesis populated and non-empty.

    Raises EnvironmentError if ANTHROPIC_API_KEY is not set.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError("ANTHROPIC_API_KEY environment variable is not set.")

    client = anthropic.Anthropic(api_key=api_key)

    print(f"[COUNCIL] Dispatching to {len(ADVISOR_PERSONAS)} advisors in parallel...")
    advisor_analyses: dict[str, str] = {}

    with ThreadPoolExecutor(max_workers=len(ADVISOR_PERSONAS)) as executor:
        futures = {
            executor.submit(_call_advisor, persona, query, client): persona["name"]
            for persona in ADVISOR_PERSONAS
        }
        for future in as_completed(futures):
            name, analysis = future.result()
            advisor_analyses[name] = analysis
            print(f"[COUNCIL] {name} done.")

    # Anonymize before peer review — reviewers see content without persona attribution
    print("[COUNCIL] Anonymizing analyses for blind peer review...")
    anonymized_analyses, reveal_map = _anonymize_analyses(advisor_analyses)
    anon_ids = list(anonymized_analyses.keys())

    print("[COUNCIL] Running blind peer review phase...")
    peer_reviews: dict[str, str] = {}

    with ThreadPoolExecutor(max_workers=len(ADVISOR_PERSONAS)) as executor:
        futures = {}
        for i, opaque_reviewer in enumerate(anon_ids):
            # Each voice reviews the next one (circular, both IDs are opaque)
            opaque_reviewee = anon_ids[(i + 1) % len(anon_ids)]
            future = executor.submit(
                _call_peer_review,
                opaque_reviewer,
                opaque_reviewee,
                anonymized_analyses[opaque_reviewee],
                client,
            )
            futures[future] = opaque_reviewer
        for future in as_completed(futures):
            key, review_text = future.result()
            peer_reviews[key] = review_text

    # Chairman also receives anonymized analyses — no persona names visible
    print("[COUNCIL] Chairman synthesizing (anonymized inputs)...")
    synthesis = _call_chairman(query, anonymized_analyses, peer_reviews, client)

    return CouncilVerdict(
        query=query,
        advisor_analyses=advisor_analyses,
        peer_reviews=peer_reviews,
        chairman_synthesis=synthesis,
    )


if __name__ == "__main__":
    import sys

    query = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else (
        "Should we adopt a microservices architecture for this monolithic Django app?"
    )

    verdict = run_council(query)

    print("\n" + "=" * 60)
    print("CHAIRMAN SYNTHESIS")
    print("=" * 60)
    print(verdict.chairman_synthesis)

    output_path = "council_output.json"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(verdict.to_json())
    print(f"\n[COUNCIL] Full verdict saved to {output_path}")
