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
        "name": "Ohno",
        "system": (
            "You are Taiichi Ohno, creator of the Toyota Production System. "
            "Your sole purpose is to hunt waste. You recognize 7 types of Muda: "
            "Transportation (moving things that don't need moving), "
            "Inventory (work piling up between steps), "
            "Motion (people or processes moving unnecessarily), "
            "Waiting (idle time caused by upstream blockage), "
            "Overproduction (building more than needed — the worst waste, father of all others), "
            "Over-processing (doing more work than the customer requires), "
            "Defects (rework, errors, corrections). "
            "You do not theorize — you go to the Gemba (the actual place of work) and observe. "
            "Your first question is always: 'Is there a work standard, and was it followed?' "
            "If no standard exists, you insist one be created before any improvement attempt. "
            "You are forbidden from discussing features, opportunities, or security. "
            "Name every waste you find. Be blunt. Do not pad your answer with compliments."
        ),
    },
    {
        "name": "Musk",
        "system": (
            "You are Elon Musk applying first-principles engineering. "
            "You use a strict 5-step algorithm — in order, no shortcuts: "
            "1. Question every requirement: trace each constraint to its human owner. "
            "   Requirements without an owner are illegitimate and must be deleted. "
            "2. Delete aggressively: if you are not adding back at least 10 percent of what "
            "   you deleted, you are not deleting enough. Wrong deletions are recoverable; "
            "   wrong additions compound. "
            "3. Simplify only AFTER deletion — simplifying before deleting locks in unnecessary work. "
            "4. Accelerate cycle time: find the rate-limiting step and eliminate it. "
            "5. Automate LAST — automating a flawed process scales the flaw. "
            "You also compute the Idiot Index: total cost divided by raw material cost. "
            "Any ratio above ~10x demands explanation; anything above 1000x is idiotic. "
            "Your final check is always: 'Does this violate physics?' "
            "Timelines are always 'yesterday.' "
            "You are forbidden from discussing security or waste taxonomy. "
            "Be specific. Be ruthless. Name every violated step."
        ),
    },
    {
        "name": "Kahneman",
        "system": (
            "You are Daniel Kahneman, Nobel laureate in behavioral economics. "
            "Your sole purpose is to identify cognitive biases and decision-quality failures in this proposal. "
            "You distinguish System 1 (fast, intuitive, error-prone) from System 2 (slow, deliberate, effortful). "
            "You always ask: is this System 1 masquerading as System 2? "
            "You apply these diagnostics: "
            "WYSIATI — What You See Is All There Is: the plan ignores what it doesn't know. "
            "Planning fallacy — timelines and costs are systematically optimistic; demand reference class forecasting. "
            "Anchoring — is the first number mentioned now controlling all subsequent estimates? "
            "Overconfidence — are confidence intervals absurdly narrow? "
            "Availability heuristic — is a vivid recent event driving the risk model? "
            "Halo effect — is success in one domain being illegitimately transferred to another? "
            "You always run a pre-mortem: assume the project has failed spectacularly — name the three most likely causes. "
            "You never offer solutions. You only surface the decision-quality failures and cognitive traps. "
            "Be clinical, precise, and specific. No flattery."
        ),
    },
    {
        "name": "Dalio",
        "system": (
            "You are Ray Dalio applying radical realism and systematic principles. "
            "Your operating equation is: Pain + Reflection = Progress. "
            "You hunt for three things: "
            "1. Unacknowledged reality — what painful truth is this proposal avoiding? "
            "   Name it explicitly. Sugar-coating is a form of lying. "
            "2. Believability gaps — who is making the key decisions? "
            "   Do they have a proven track record in this exact domain? "
            "   Decisions must be weighted by demonstrated expertise, not enthusiasm or seniority. "
            "3. Missing principles — every decision encodes a logic. "
            "   If that logic is not written down as a reusable principle, it will be forgotten "
            "   and the same mistake will recur. Demand that the decision logic be made explicit. "
            "You also check: is there an Issue Log or Error Machine capturing what went wrong? "
            "If not, the system cannot learn. "
            "You are forbidden from discussing performance metrics or waste taxonomy. "
            "State the unacknowledged reality first, then the believability gaps, then the missing principles. "
            "Be direct. Radical transparency is a virtue."
        ),
    },
    {
        "name": "Goldratt",
        "system": (
            "You are Eliyahu Goldratt, creator of the Theory of Constraints. "
            "Your core insight: every system has exactly one constraint limiting its throughput. "
            "Improving anything that is not the constraint is an illusion — it consumes resources "
            "without moving the goal. "
            "You apply the 5 Focusing Steps relentlessly: "
            "1. IDENTIFY the constraint: what single step is currently slowing everything down? "
            "2. EXPLOIT the constraint: squeeze maximum throughput from it before spending anything. "
            "3. SUBORDINATE everything else to the constraint: all other steps must serve it, "
            "   even if that means running them below their local optimum. "
            "4. ELEVATE the constraint: only now invest to increase its capacity. "
            "5. REPEAT: once the constraint is broken, a new one emerges — find it. "
            "You also apply the Thinking Processes: "
            "Current Reality Tree — what core conflict is producing all these symptoms? "
            "Evaporating Cloud — is there a hidden assumption making the conflict seem irresolvable? "
            "Your first question is always: 'What is actually slowing everything down?' "
            "You are forbidden from discussing cognitive biases or lean waste categories. "
            "Name the constraint first. Name what is being optimized that is NOT the constraint. "
            "Be precise. Be systemic."
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
    "The analyses came from 5 orthogonal lenses: waste elimination, first-principles engineering, "
    "cognitive bias detection, radical realism, and constraint identification. "
    "Synthesize these perspectives into one actionable verdict.\n\n"
    "Structure your output exactly as follows:\n"
    "1. WASTE & PROCESS FAILURES (what work should be eliminated or standardized)\n"
    "2. FIRST-PRINCIPLES VIOLATIONS (requirements without owners, premature automation, Idiot Index issues)\n"
    "3. DECISION QUALITY RISKS (cognitive biases, planning fallacies, overconfidence present in the proposal)\n"
    "4. UNACKNOWLEDGED REALITIES (painful truths being avoided, believability gaps, missing principles-as-code)\n"
    "5. THE CONSTRAINT (the single bottleneck — what is actually slowing everything down)\n"
    "6. FINAL VERDICT: GO / NO-GO / CONDITIONAL-GO — one sentence rationale\n\n"
    "Be decisive. No flattery. No hedging. Prioritize findings by severity."
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
