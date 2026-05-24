"""
GitHub Project Issue Runner
Reads items from the GitHub Projects board, executes each via Claude Code CLI,
and moves items through the board (Ready → In Progress → Done).

Usage:
    python issue_runner.py           # process all Ready items once, then exit
    python issue_runner.py --watch   # poll every 60s continuously

How it works:
    1. Fetches items with status "Ready" from the configured project board
    2. Moves the item to "In Progress"
    3. Runs: claude -p "<AGENT_CONTEXT + issue title + body>"
    4. Moves the item to "Done" (or "Review" on failure)

Claude Code CLI interprets the issue body and executes whatever is needed —
sending emails, running scripts, querying APIs, reading files, etc.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import datetime

# ---------------------------------------------------------------------------
# Config — GitHub project coordinates
# ---------------------------------------------------------------------------
OWNER = "nlebid32-png"
PROJECT_NUMBER = 1
PROJECT_ID = "PVT_kwHOEJXjQc4BYnxQ"
STATUS_FIELD_ID = "PVTSSF_lAHOEJXjQc4BYnxQzhTsSnc"

STATUS_OPTIONS = {
    "Backlog":     "d6a65d2b",
    "Ready":       "f75ad846",
    "In Progress": "47fc9ee4",
    "Review":      "0af9fe86",
    "Done":        "98236657",
}

# ---------------------------------------------------------------------------
# Context injected into every claude -p call
# ---------------------------------------------------------------------------
AGENT_CONTEXT = """\
You are an autonomous agent executing a task from the GitHub Projects board for nlebid32-png.

Available projects and tools you can use:
- karpathy-agentic-engineering: LLM Council (5 domain-grounded advisors), diversity_check,
  autonomous loop toolkit. Path: G:\\My Drive\\Claude work folder\\karpathy-agentic-engineering
- canvas-ai-pipeline: Canvas LMS assignment processing pipeline (Flask web app + AI processor).
  Path: G:\\My Drive\\Claude work folder\\canvas-ai-pipeline
- obsidian-vault-agent: Daily Gmail/Calendar → Obsidian vault pipeline.
  Path: G:\\My Drive\\Claude work folder\\obsidian-vault-agent
- Gmail MCP: Can send and read emails for nlebid32@gmail.com
- Google Calendar MCP: Can read/create calendar events
- gh CLI: Fully authenticated as nlebid32-png — can read repos, issues, projects
- Python: All project dependencies installed

User email: nlebid32@gmail.com

Complete the task fully. When done, write a 1-2 sentence summary of what you did.\
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
    """Return all project items currently in Ready status."""
    raw = _gh("project", "item-list", str(PROJECT_NUMBER),
              "--owner", OWNER, "--format", "json")
    data = json.loads(raw)
    return [item for item in data["items"] if item.get("status") == "Ready"]


def set_status(item_id: str, status: str) -> None:
    """Move a project item to the given status column."""
    option_id = STATUS_OPTIONS[status]
    _gh(
        "project", "item-edit",
        "--project-id", PROJECT_ID,
        "--id", item_id,
        "--field-id", STATUS_FIELD_ID,
        "--single-select-option-id", option_id,
    )
    print(f"  [BOARD] -> {status}")


def add_issue_comment(repo: str, issue_number: int, body: str) -> None:
    """Post an execution summary comment on the source issue."""
    try:
        repo_short = repo.replace("https://github.com/", "")
        _gh("issue", "comment", str(issue_number), "--repo", repo_short, "--body", body)
        print(f"  [COMMENT] Posted to issue #{issue_number}")
    except RuntimeError as e:
        print(f"  [COMMENT] Could not post comment: {e}")


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------

def execute_item(item: dict) -> bool:
    """
    Execute a project item by calling claude -p with the full context + issue body.
    Returns True on success (exit code 0), False on failure.
    """
    content = item.get("content", {})
    title = item.get("title", "(no title)")
    body = content.get("body") or "(no description provided)"
    repo = content.get("repository", "")
    issue_number = content.get("number")

    prompt = (
        f"{AGENT_CONTEXT}\n\n"
        f"---\n"
        f"TASK FROM GITHUB PROJECT BOARD\n"
        f"Title: {title}\n"
        f"Repository: {repo}\n"
        f"Issue: #{issue_number}\n\n"
        f"Instructions:\n{body.strip()}\n"
        f"---\n\n"
        f"Execute the task above now."
    )

    print(f"\n  [EXEC] Running: claude -p ...")
    import shutil
    claude_bin = shutil.which("claude") or r"C:\Users\Thicc\AppData\Roaming\npm\claude.cmd"
    result = subprocess.run(
        [claude_bin, "-p", prompt, "--allowedTools", "all"],
        text=True,
        encoding="utf-8",
    )
    success = result.returncode == 0

    if issue_number and repo:
        status_line = "Task completed successfully." if success else "Task failed (non-zero exit)."
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        comment = f"**Issue Runner** executed this task at {timestamp}.\n\n{status_line}"
        add_issue_comment(repo, issue_number, comment)

    return success


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def process_once() -> int:
    """Process all Ready items. Returns number of items processed."""
    items = fetch_ready_items()
    if not items:
        print("[RUNNER] No items in Ready. Nothing to do.")
        return 0

    print(f"[RUNNER] Found {len(items)} Ready item(s).")
    processed = 0

    for item in items:
        item_id = item["id"]
        title = item.get("title", "(no title)")
        print(f"\n[RUNNER] Processing: {title}")

        set_status(item_id, "In Progress")
        try:
            success = execute_item(item)
            final_status = "Done" if success else "Review"
        except Exception as e:
            print(f"  [ERROR] {e}")
            final_status = "Review"

        set_status(item_id, final_status)
        processed += 1

    return processed


def run(watch: bool = False, interval_seconds: int = 60) -> None:
    if watch:
        print(f"[RUNNER] Watch mode — polling every {interval_seconds}s. Ctrl+C to stop.")
        while True:
            try:
                process_once()
            except KeyboardInterrupt:
                print("\n[RUNNER] Stopped.")
                break
            except Exception as e:
                print(f"[RUNNER] Error: {e}")
            time.sleep(interval_seconds)
    else:
        process_once()


if __name__ == "__main__":
    watch_mode = "--watch" in sys.argv
    run(watch=watch_mode)
