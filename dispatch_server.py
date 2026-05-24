"""
Dispatch Server — mobile-friendly task creation for the GitHub Project board.

Usage:
    python dispatch_server.py
    -> Open http://<your-local-ip>:5051 on your phone

How it works:
    1. You type or voice-dictate a task on your phone
    2. Claude formats it into a title + description
    3. A GitHub issue is created and added to the project board
    4. The issue_runner picks it up within 60 seconds and executes it
"""
from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime

import anthropic
from dotenv import load_dotenv
from flask import Flask, jsonify, request

load_dotenv("G:/My Drive/Claude work folder/canvas-ai-pipeline/.env")

app = Flask(__name__)

OWNER = "nlebid32-png"
PROJECT_NUMBER = 1
REPO = "nlebid32-png/karpathy-agentic-engineering"

client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))


def _gh(*args: str) -> str:
    result = subprocess.run(
        ["gh"] + list(args), capture_output=True, text=True, encoding="utf-8"
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())
    return result.stdout.strip()


def format_task(raw: str) -> dict:
    """Use Claude to parse natural language into a task title + description."""
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=300,
        system=(
            "You convert natural language task requests into structured GitHub issues. "
            "Always respond with valid JSON only — no markdown, no explanation.\n"
            'Format: {"title": "short imperative title (max 60 chars)", '
            '"body": "clear instructions for an AI agent to execute this task. '
            "Include all context from the user's message. "
            '2-4 sentences max."}'
        ),
        messages=[{"role": "user", "content": raw}],
    )
    text = response.content[0].text.strip()
    return json.loads(text)


def create_issue(title: str, body: str) -> dict:
    """Create a GitHub issue and add it to the project board."""
    result = _gh(
        "issue", "create",
        "--repo", REPO,
        "--title", title,
        "--body", body,
        "--json", "number,url",
    )
    issue = json.loads(result)

    _gh(
        "project", "item-add", str(PROJECT_NUMBER),
        "--owner", OWNER,
        "--url", issue["url"],
    )

    return issue


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return DISPATCH_HTML


@app.route("/dispatch", methods=["POST"])
def dispatch():
    data = request.get_json(force=True)
    raw = (data.get("message") or "").strip()
    if not raw:
        return jsonify({"error": "No message provided."}), 400

    try:
        task = format_task(raw)
    except Exception as e:
        return jsonify({"error": f"Could not parse task: {e}"}), 500

    try:
        issue = create_issue(task["title"], task["body"])
    except Exception as e:
        return jsonify({"error": f"Could not create issue: {e}"}), 500

    return jsonify({
        "title": task["title"],
        "body": task["body"],
        "issue_number": issue["number"],
        "url": issue["url"],
        "message": f"Task #{issue['number']} added to the board. Agent picks it up within 60 seconds.",
    })


@app.route("/status")
def status():
    result = subprocess.run(
        ["gh", "project", "item-list", str(PROJECT_NUMBER),
         "--owner", OWNER, "--format", "json"],
        capture_output=True, text=True, encoding="utf-8"
    )
    if result.returncode != 0:
        return jsonify({"error": result.stderr}), 500
    items = json.loads(result.stdout).get("items", [])
    return jsonify({"items": [
        {"title": i["title"], "status": i.get("status", "?")}
        for i in items
    ]})


# ---------------------------------------------------------------------------
# Mobile UI (served inline — no template files needed)
# ---------------------------------------------------------------------------

DISPATCH_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
<title>Dispatch</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    background: #0d1117;
    color: #e6edf3;
    min-height: 100vh;
    display: flex;
    flex-direction: column;
  }
  header {
    padding: 20px 16px 12px;
    border-bottom: 1px solid #21262d;
    display: flex;
    align-items: center;
    gap: 10px;
  }
  header h1 { font-size: 18px; font-weight: 600; }
  header .dot {
    width: 8px; height: 8px; border-radius: 50%;
    background: #3fb950; box-shadow: 0 0 6px #3fb950;
  }
  #log {
    flex: 1;
    overflow-y: auto;
    padding: 16px;
    display: flex;
    flex-direction: column;
    gap: 12px;
  }
  .msg {
    max-width: 85%;
    padding: 10px 14px;
    border-radius: 16px;
    font-size: 15px;
    line-height: 1.4;
    word-break: break-word;
  }
  .msg.user {
    background: #1f6feb;
    align-self: flex-end;
    border-bottom-right-radius: 4px;
  }
  .msg.agent {
    background: #161b22;
    border: 1px solid #30363d;
    align-self: flex-start;
    border-bottom-left-radius: 4px;
  }
  .msg.agent .label {
    font-size: 11px;
    color: #8b949e;
    margin-bottom: 4px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }
  .msg.agent .title { font-weight: 600; color: #58a6ff; margin-bottom: 2px; }
  .msg.agent .detail { font-size: 13px; color: #8b949e; }
  .msg.error { background: #3d1f1f; border: 1px solid #6e1f1f; align-self: flex-start; }
  .typing { display: flex; gap: 5px; align-items: center; padding: 10px 14px; }
  .typing span {
    width: 7px; height: 7px; border-radius: 50%;
    background: #8b949e; animation: bounce 1.2s infinite;
  }
  .typing span:nth-child(2) { animation-delay: 0.2s; }
  .typing span:nth-child(3) { animation-delay: 0.4s; }
  @keyframes bounce {
    0%, 80%, 100% { transform: translateY(0); }
    40% { transform: translateY(-6px); }
  }
  footer {
    padding: 12px 16px;
    border-top: 1px solid #21262d;
    background: #0d1117;
    display: flex;
    gap: 8px;
    align-items: flex-end;
  }
  #input {
    flex: 1;
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 20px;
    padding: 10px 16px;
    color: #e6edf3;
    font-size: 16px;
    resize: none;
    min-height: 44px;
    max-height: 120px;
    outline: none;
    line-height: 1.4;
  }
  #input:focus { border-color: #58a6ff; }
  #input::placeholder { color: #484f58; }
  button {
    width: 44px; height: 44px;
    border-radius: 50%;
    border: none;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    font-size: 18px;
    transition: opacity 0.15s;
  }
  button:active { opacity: 0.7; }
  #sendBtn { background: #1f6feb; }
  #micBtn { background: #21262d; }
  #micBtn.recording { background: #da3633; animation: pulse 1s infinite; }
  @keyframes pulse {
    0%, 100% { box-shadow: 0 0 0 0 rgba(218,54,51,0.4); }
    50% { box-shadow: 0 0 0 8px rgba(218,54,51,0); }
  }
  #status-bar {
    padding: 6px 16px;
    font-size: 12px;
    color: #8b949e;
    border-bottom: 1px solid #21262d;
    display: flex;
    gap: 16px;
    overflow-x: auto;
  }
  .status-chip {
    display: flex; align-items: center; gap: 5px;
    white-space: nowrap;
  }
  .status-chip .s { width: 6px; height: 6px; border-radius: 50%; }
  .s-backlog { background: #484f58; }
  .s-ready { background: #d29922; }
  .s-progress { background: #1f6feb; }
  .s-review { background: #da3633; }
  .s-done { background: #3fb950; }
</style>
</head>
<body>

<header>
  <div class="dot"></div>
  <h1>Dispatch</h1>
</header>

<div id="status-bar"></div>

<div id="log">
  <div class="msg agent">
    <div class="label">Dispatch</div>
    What do you need done?
  </div>
</div>

<footer>
  <textarea id="input" placeholder="Describe a task..." rows="1"
    onInput="autoResize(this)" onKeyDown="handleKey(event)"></textarea>
  <button id="micBtn" onclick="toggleMic()" title="Voice input">🎤</button>
  <button id="sendBtn" onclick="send()" title="Send">&#10148;</button>
</footer>

<script>
const log = document.getElementById('log');
const input = document.getElementById('input');
const micBtn = document.getElementById('micBtn');
const statusBar = document.getElementById('status-bar');

function autoResize(el) {
  el.style.height = 'auto';
  el.style.height = Math.min(el.scrollHeight, 120) + 'px';
}

function handleKey(e) {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send(); }
}

function addMsg(html, cls) {
  const d = document.createElement('div');
  d.className = 'msg ' + cls;
  d.innerHTML = html;
  log.appendChild(d);
  log.scrollTop = log.scrollHeight;
  return d;
}

function showTyping() {
  const d = document.createElement('div');
  d.className = 'msg agent';
  d.innerHTML = '<div class="typing"><span></span><span></span><span></span></div>';
  log.appendChild(d);
  log.scrollTop = log.scrollHeight;
  return d;
}

async function send() {
  const text = input.value.trim();
  if (!text) return;
  addMsg(text, 'user');
  input.value = '';
  autoResize(input);
  const typing = showTyping();

  try {
    const res = await fetch('/dispatch', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({message: text})
    });
    const data = await res.json();
    typing.remove();
    if (data.error) {
      addMsg('<div class="label">Error</div>' + data.error, 'agent error');
    } else {
      addMsg(
        '<div class="label">Task #' + data.issue_number + ' created</div>' +
        '<div class="title">' + data.title + '</div>' +
        '<div class="detail">' + data.message + '</div>',
        'agent'
      );
      loadStatus();
    }
  } catch(e) {
    typing.remove();
    addMsg('<div class="label">Error</div>Could not reach server.', 'agent error');
  }
}

// Voice input
let recognition = null;
function toggleMic() {
  const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
  if (!SpeechRecognition) {
    alert('Voice input not supported in this browser. Try Chrome on Android or Safari on iOS.');
    return;
  }
  if (recognition) {
    recognition.stop();
    recognition = null;
    micBtn.classList.remove('recording');
    return;
  }
  recognition = new SpeechRecognition();
  recognition.continuous = false;
  recognition.interimResults = false;
  recognition.lang = 'en-US';
  recognition.onresult = e => {
    input.value = e.results[0][0].transcript;
    autoResize(input);
    micBtn.classList.remove('recording');
    recognition = null;
    send();
  };
  recognition.onerror = () => {
    micBtn.classList.remove('recording');
    recognition = null;
  };
  recognition.onend = () => {
    micBtn.classList.remove('recording');
    recognition = null;
  };
  recognition.start();
  micBtn.classList.add('recording');
}

// Status bar
const STATUS_COLORS = {
  'Backlog':'s-backlog','Ready':'s-ready',
  'In Progress':'s-progress','Review':'s-review','Done':'s-done'
};
async function loadStatus() {
  try {
    const res = await fetch('/status');
    const data = await res.json();
    if (!data.items) return;
    const recent = data.items.slice(0, 6);
    statusBar.innerHTML = recent.map(i =>
      '<div class="status-chip">' +
      '<div class="s ' + (STATUS_COLORS[i.status]||'s-backlog') + '"></div>' +
      i.title.slice(0, 28) + (i.title.length > 28 ? '…' : '') +
      '</div>'
    ).join('');
  } catch(e) {}
}

loadStatus();
setInterval(loadStatus, 15000);
</script>
</body>
</html>
"""


if __name__ == "__main__":
    import socket
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    print(f"Dispatch server running:")
    print(f"  Local:   http://localhost:5051")
    print(f"  Network: http://{local_ip}:5051  <- open this on your phone")
    app.run(host="0.0.0.0", port=5051, debug=False)
