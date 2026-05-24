"""
GitHub Project Issue Runner
Monitors the GitHub Projects board and executes Ready items via Claude Code CLI.

Modes:
    python issue_runner.py                  # process all Ready items once, exit
    python issue_runner.py --watch          # poll every 60s (set POLL_INTERVAL to change)
    python issue_runner.py --setup          # install Windows Task Scheduler job (run once)

Epic decomposition:
    If an issue title or body starts with [EPIC], the runner calls Claude to break it
    into sub-issues, creates each on GitHub, adds them to the project board as Ready,
    and processes them in parallel with up to MAX_PARALLEL_AGENTS concurrent agents.

Board flow:
    Ready -> In Progress -> Done   (success)
    Ready -> In Progress -> Review (failure — needs human attention)
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
OWNER = "nlebid32-png"
PROJECT_NUMBER = 1
PROJECT_ID = "PVT_kwHOEJXjQc4BYnxQ"
STATUS_FIELD_ID = "PVTSSF_lAHOEJXjQc4BYnxQzhTsSnc"
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "60"))  # seconds
MAX_PARALLEL_AGENTS = 3

STATUS_OPTIONS = {
    "Backlog":     "d6a65d2b",
    "Ready":       "f75ad846",
    "In Progress": "47fc9ee4",
    "Review":      "0af9fe86",
    "Done":        "98236657",
}

CLAUDE_BIN = (
    subprocess.run(["where", "claude"], capture_output=True, text=True).stdout.strip().split("\n")[0]
    or r"C:\Users\Thicc\AppData\Roaming\npm\claude.cmd"
)

# ---------------------------------------------------------------------------
# Agent context injected into every claude -p call
# ---------------------------------------------------------------------------
AGENT_CONTEXT = """\
You are an autonomous agent executing a task from the GitHub Projects board for nlebid32-png.

Available projects and tools:
- karpathy-agentic-engineering: LLM Council (Ohno/Musk/Kahneman/Dalio/Goldratt personas),
  diversity_check, autonomous loop toolkit, issue_runner.
  Path: G:\\My Drive\\Claude work folder\\karpathy-agentic-engineering
- canvas-ai-pipeline: Canvas LMS assignment processing pipeline (Flask + AI processor).
  Path: G:\\My Drive\\Claude work folder\\canvas-ai-pipeline
- obsidian-vault-agent: Daily Gmail/Calendar -> Obsidian vault pipeline.
  Path: G:\\My Drive\\Claude work folder\\obsidian-vault-agent
- agent-appstore: Flask dashboard at localhost:5050 monitoring all agents.
  Path: G:\\My Drive\\Claude work folder\\agent-appstore
- Gmail MCP: Can send and read emails for nlebid32@gmail.com
- Google Calendar MCP: Can read/create calendar events
- gh CLI: Authenticated as nlebid32-png (scopes: repo, project)
- Python: All project dependencies installed
- GitHub Project board: owner=nlebid32-png, project=1

User email: nlebid32@gmail.com

Complete the task fully. At the end, write a 1-2 sentence plain-English summary of what you did.\
"""


# ---------------------------------------------------------------------------
# gh CLI wrapper
# ---------------------------------------------------------------------------

def _gh(*args: str) -> str:
    result = subprocess.run(
        ["gh"] + list(args),
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        raise RuntimeError(f"gh {' '.join(args)} failed:\n{result.stderr.strip()}")
    return result.stdout.strip()


# ---------------------------------------------------------------------------
# Board operations
# ---------------------------------------------------------------------------

def fetch_ready_items() -> list[dict]:
    raw = _gh("project", "item-list", str(PROJECT_NUMBER),
              "--owner", OWNER, "--format", "json")
    data = json.loads(raw)
    return [item for item in data["items"] if item.get("status") == "Ready"]


def set_status(item_id: str, status: str) -> None:
    _gh(
        "project", "item-edit",
        "--project-id", PROJECT_ID,
        "--id", item_id,
        "--field-id", STATUS_FIELD_ID,
        "--single-select-option-id", STATUS_OPTIONS[status],
    )
    print(f"  [BOARD] -> {status}")


def add_item_to_board(issue_url: str) -> str:
    """Add a GitHub issue URL to the project board. Returns the new item ID."""
    result = _gh(
        "project", "item-add", str(PROJECT_NUMBER),
        "--owner", OWNER,
        "--url", issue_url,
        "--format", "json",
    )
    return json.loads(result).get("id", "")


def post_comment(repo_short: str, issue_number: int, body: str) -> None:
    try:
        _gh("issue", "comment", str(issue_number), "--repo", repo_short, "--body", body)
        print(f"  [COMMENT] Posted to issue #{issue_number}")
    except RuntimeError as e:
        print(f"  [COMMENT] Skipped: {e}")


# ---------------------------------------------------------------------------
# Epic detection
# ---------------------------------------------------------------------------

def is_epic(item: dict) -> bool:
    """Return True if the issue is flagged as an epic to be decomposed."""
    title = item.get("title", "")
    body = item.get("content", {}).get("body") or ""
    return title.strip().startswith("[EPIC]") or body.strip().startswith("[EPIC]")


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------

def _run_claude(prompt: str) -> int:
    """Run claude -p with the given prompt. Returns exit code."""
    result = subprocess.run(
        [CLAUDE_BIN, "-p", prompt, "--allowedTools", "all"],
        text=True,
        encoding="utf-8",
    )
    return result.returncode


def execute_regular(item: dict) -> bool:
    """Execute a standard (non-epic) issue. Returns True on success."""
    content = item.get("content", {})
    title = item.get("title", "(no title)")
    body = (content.get("body") or "(no description provided)").strip()
    repo = content.get("repository", "")
    issue_number = content.get("number")
    repo_short = repo.replace("https://github.com/", "") if repo else ""

    prompt = (
        f"{AGENT_CONTEXT}\n\n"
        f"---\n"
        f"TASK FROM GITHUB PROJECT BOARD\n"
        f"Title: {title}\n"
        f"Repository: {repo}\n"
        f"Issue #: {issue_number}\n\n"
        f"Instructions:\n{body}\n"
        f"---\n\n"
        f"Execute the task above now. Use all tools available to you."
    )

    print(f"  [EXEC] Dispatching to Claude...")
    code = _run_claude(prompt)
    success = code == 0

    if repo_short and issue_number:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M")
        status_word = "completed" if success else "failed (exit code: non-zero)"
        post_comment(
            repo_short,
            issue_number,
            f"**Issue Runner** — {ts}\n\nExecution {status_word}.",
        )

    return success


def decompose_and_execute_epic(item: dict) -> bool:
    """
    For [EPIC] issues:
    1. Call Claude to decompose into sub-issues and create them on GitHub
    2. Collect the new Ready items from the board
    3. Execute sub-issues in parallel (up to MAX_PARALLEL_AGENTS)
    """
    content = item.get("content", {})
    title = item.get("title", "(no title)")
    body = (content.get("body") or "").strip()
    repo = content.get("repository", "")
    issue_number = content.get("number")
    repo_short = repo.replace("https://github.com/", "") if repo else ""

    print(f"  [EPIC] Decomposing into sub-issues...")

    decompose_prompt = (
        f"{AGENT_CONTEXT}\n\n"
        f"---\n"
        f"EPIC DECOMPOSITION TASK\n"
        f"Epic title: {title}\n"
        f"Epic body:\n{body}\n"
        f"Repository: {repo_short or 'nlebid32-png/karpathy-agentic-engineering'}\n"
        f"Project: owner={OWNER}, number={PROJECT_NUMBER}\n"
        f"---\n\n"
        f"Break this epic into 3-7 discrete, independently executable sub-tasks.\n"
        f"For each sub-task:\n"
        f"  1. Create a GitHub issue:\n"
        f"     gh issue create --repo <repo> --title '<title>' --body '<clear instructions>'\n"
        f"  2. Add it to the project board as Ready:\n"
        f"     gh project item-add {PROJECT_NUMBER} --owner {OWNER} --url <issue_url>\n\n"
        f"Rules for sub-issues:\n"
        f"- Each must be self-contained and executable by a Claude agent with no prior context\n"
        f"- Body must include all information needed to complete the task\n"
        f"- Keep scope to < 30 minutes of work each\n"
        f"- Do NOT start executing the sub-issues — only create them\n\n"
        f"After creating all sub-issues, print a plain list of the issue URLs created."
    )

    print(f"  [EPIC] Calling Claude to decompose...")
    code = _run_claude(decompose_prompt)
    if code != 0:
        print(f"  [EPIC] Decomposition failed.")
        return False

    if repo_short and issue_number:
        post_comment(
            repo_short,
            issue_number,
            f"**Issue Runner** — {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
            f"Epic decomposed. Sub-issues created and added to the project board as Ready. "
            f"Parallel agents are now processing them.",
        )

    print(f"  [EPIC] Waiting 5s for board to update...")
    time.sleep(5)

    sub_items = fetch_ready_items()
    if not sub_items:
        print(f"  [EPIC] No sub-issues found in Ready after decomposition.")
        return True

    print(f"  [EPIC] Running {len(sub_items)} sub-issue(s) in parallel (max {MAX_PARALLEL_AGENTS} agents)...")
    return _run_parallel(sub_items)


def _run_parallel(items: list[dict]) -> bool:
    """Execute multiple items in parallel. Returns True if all succeeded."""
    all_success = True

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_AGENTS) as executor:
        futures = {executor.submit(_process_single_item, item): item for item in items}
        for future in as_completed(futures):
            item = futures[future]
            try:
                success = future.result()
                if not success:
                    all_success = False
            except Exception as e:
                print(f"  [PARALLEL] Error on '{item.get('title')}': {e}")
                all_success = False

    return all_success


def _process_single_item(item: dict) -> bool:
    """Move item through the board and execute it. Used by both serial and parallel runs."""
    item_id = item["id"]
    title = item.get("title", "(no title)")
    print(f"\n  [ITEM] Starting: {title}")
    set_status(item_id, "In Progress")
    try:
        success = execute_regular(item)
        final_status = "Done" if success else "Review"
    except Exception as e:
        print(f"  [ITEM] Error: {e}")
        final_status = "Review"
        success = False
    set_status(item_id, final_status)
    return success


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------

def process_once() -> int:
    """Fetch and process all Ready items. Returns count processed."""
    items = fetch_ready_items()
    if not items:
        print(f"[{_ts()}] No items in Ready.")
        return 0

    print(f"[{_ts()}] Found {len(items)} Ready item(s).")

    epics = [i for i in items if is_epic(i)]
    regular = [i for i in items if not is_epic(i)]

    # Process regular items in parallel
    if regular:
        print(f"[RUNNER] {len(regular)} regular item(s) -> parallel execution")
        for item in regular:
            set_status(item["id"], "In Progress")
        _run_parallel(regular)

    # Process epics serially (each spawns its own parallel sub-agents)
    for item in epics:
        item_id = item["id"]
        title = item.get("title", "(no title)")
        print(f"\n[RUNNER] [EPIC] {title}")
        set_status(item_id, "In Progress")
        try:
            success = decompose_and_execute_epic(item)
            set_status(item_id, "Done" if success else "Review")
        except Exception as e:
            print(f"  [EPIC] Error: {e}")
            set_status(item_id, "Review")

    return len(items)


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


# ---------------------------------------------------------------------------
# Scheduler setup (Windows Task Scheduler)
# ---------------------------------------------------------------------------

def setup_scheduler() -> None:
    """
    Register a Windows Task Scheduler job that runs issue_runner.py --watch
    at system startup, restarting automatically if it crashes.
    """
    runner_dir = os.path.dirname(os.path.abspath(__file__))
    bat_path = os.path.join(runner_dir, "run_issues_watch.bat")

    with open(bat_path, "w") as f:
        f.write(f'@echo off\n')
        f.write(f'cd /d "{runner_dir}"\n')
        f.write(f':loop\n')
        f.write(f'python issue_runner.py --watch\n')
        f.write(f'echo [Restart] Issue runner exited. Restarting in 10s...\n')
        f.write(f'timeout /t 10 /nobreak\n')
        f.write(f'goto loop\n')

    task_name = "ClaudeIssueRunner"
    cmd = [
        "schtasks", "/create", "/f",
        "/tn", task_name,
        "/tr", bat_path,
        "/sc", "ONLOGON",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"[SETUP] Task '{task_name}' registered. It will start on next login.")
        print(f"[SETUP] To start now: schtasks /run /tn {task_name}")
        print(f"[SETUP] To remove: schtasks /delete /tn {task_name} /f")

        start = subprocess.run(
            ["schtasks", "/run", "/tn", task_name],
            capture_output=True, text=True,
        )
        if start.returncode == 0:
            print(f"[SETUP] Runner started now in background.")
        else:
            print(f"[SETUP] Could not auto-start: {start.stderr.strip()}")
    else:
        print(f"[SETUP] Failed: {result.stderr.strip()}")
        print(f"[SETUP] You can run manually: python issue_runner.py --watch")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_watch() -> None:
    print(f"[RUNNER] Watch mode — polling every {POLL_INTERVAL}s. Ctrl+C to stop.")
    while True:
        try:
            process_once()
        except KeyboardInterrupt:
            print("\n[RUNNER] Stopped.")
            break
        except Exception as e:
            print(f"[RUNNER] Error: {e}")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    args = sys.argv[1:]
    if "--setup" in args:
        setup_scheduler()
    elif "--watch" in args:
        run_watch()
    else:
        process_once()
