"""
orchestra_tool.py — Hermes built-in tool for the orchestra.

Drop this file into hermes-agent/tools/ to expose orchestra commands
directly as LLM-callable tools inside Hermes.

Tools registered:
  orchestra_submit   — submit a task to the orchestra
  orchestra_status   — get system + agent status
  orchestra_result   — fetch a task result
  orchestra_agents   — list all agents with their soul/skill summaries
  orchestra_metrics  — latency stats
  orchestra_agent_info — soul + skill detail for one agent
  orchestra_add_agent  — add a new domain agent dynamically
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from pathlib import Path

log = logging.getLogger("orchestra.tool")

# Path to orchestra_ctl.py — resolve relative to this file's location
_SCRIPTS_DIR = Path(__file__).parent.parent / "skills" / "orchestra" / "scripts"
_CTL = str(_SCRIPTS_DIR / "orchestra_ctl.py")
_PYTHON = sys.executable


def _run_ctl(*args) -> dict:
    """Run orchestra_ctl.py with given args and return {output, error, returncode}."""
    env = os.environ.copy()
    # Ensure scripts dir is on PYTHONPATH so imports work
    env["PYTHONPATH"] = str(_SCRIPTS_DIR) + os.pathsep + env.get("PYTHONPATH", "")
    try:
        result = subprocess.run(
            [_PYTHON, _CTL, *args],
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
        )
        return {
            "output": result.stdout.strip(),
            "error": result.stderr.strip() if result.returncode != 0 else "",
            "returncode": result.returncode,
        }
    except subprocess.TimeoutExpired:
        return {"output": "", "error": "Command timed out after 30s", "returncode": 1}
    except Exception as exc:
        return {"output": "", "error": str(exc), "returncode": 1}


def _check_orchestra_requirements() -> bool:
    """Return True if Redis env var is set and redis package is available."""
    if not os.environ.get("REDIS_URL"):
        return False
    try:
        import redis  # noqa
        return True
    except ImportError:
        return False


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------

def _resolve_notify_target(args: dict, kwargs: dict) -> str:
    """
    Если LLM-агент передал notify_target явно — берём его.
    Иначе пытаемся вытащить chat_id из Hermes context (kwargs).
    Hermes по соглашению может прокинуть chat_id, user_id, channel, context.
    """
    explicit = args.get("notify_target") or args.get("notify")
    if explicit:
        return str(explicit)

    # Авто-извлечение из Hermes context — поддерживаем несколько форм
    for k in ("chat_id", "tg_chat_id", "telegram_chat_id"):
        if k in kwargs and kwargs[k]:
            return f"tg:{kwargs[k]}"
        ctx = kwargs.get("context") or {}
        if isinstance(ctx, dict) and ctx.get(k):
            return f"tg:{ctx[k]}"
    # ничего не нашли — без push
    return ""


def handle_orchestra_submit(args: dict, **kwargs) -> str:
    task = args.get("task", "").strip()
    if not task:
        return json.dumps({"error": "task is required"})

    cmd_args = ["submit", task]
    notify_target = _resolve_notify_target(args, kwargs)
    if notify_target:
        cmd_args += ["--notify", notify_target]

    result = _run_ctl(*cmd_args)
    if result["returncode"] != 0:
        return json.dumps({"error": result["error"] or "submit failed"})

    # Парсим task_id из вывода
    task_id = ""
    for line in result["output"].splitlines():
        if "Задача принята:" in line or line.startswith("Task submitted:"):
            task_id = line.split(":", 1)[1].strip()
            break

    return json.dumps({
        "task_id": task_id,
        "notify":  notify_target or None,
        "message": result["output"],
    }, ensure_ascii=False)


def handle_orchestra_tasks_active(args: dict, **kwargs) -> str:
    result = _run_ctl("tasks-active", "--json")
    return json.dumps({"output": result["output"]})


def handle_orchestra_tasks_by_date(args: dict, **kwargs) -> str:
    date = args.get("date", "today").strip()
    result = _run_ctl("tasks-by-date", date, "--json")
    return json.dumps({"output": result["output"]})


def handle_orchestra_status(args: dict, **kwargs) -> str:
    result = _run_ctl("status")
    return json.dumps({"output": result["output"], "error": result.get("error", "")})


def handle_orchestra_result(args: dict, **kwargs) -> str:
    task_id = args.get("task_id", "").strip()
    if not task_id:
        return json.dumps({"error": "task_id is required"})
    result = _run_ctl("result", task_id)
    return json.dumps({"output": result["output"], "error": result.get("error", "")})


def handle_orchestra_agents(args: dict, **kwargs) -> str:
    result = _run_ctl("agents")
    return json.dumps({"output": result["output"]})


def handle_orchestra_metrics(args: dict, **kwargs) -> str:
    result = _run_ctl("metrics")
    return json.dumps({"output": result["output"]})


def handle_orchestra_agent_info(args: dict, **kwargs) -> str:
    agent_id = args.get("agent_id", "").strip()
    if not agent_id:
        return json.dumps({"error": "agent_id is required"})
    result = _run_ctl("agent-info", agent_id)
    return json.dumps({"output": result["output"]})


def handle_orchestra_add_agent(args: dict, **kwargs) -> str:
    direction = args.get("direction", "").strip()
    soul      = args.get("soul", "").strip()
    skill     = args.get("skill", "").strip()
    values    = args.get("values", "").strip()
    if not all([direction, soul, skill]):
        return json.dumps({"error": "direction, soul, and skill are required"})
    cmd = ["add-agent", "--direction", direction, "--soul", soul, "--skill", skill]
    if values:
        cmd += ["--values", values]
    result = _run_ctl(*cmd)
    return json.dumps({"output": result["output"], "error": result.get("error", "")})


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

SCHEMAS = {
    "orchestra_submit": {
        "name": "orchestra_submit",
        "description": (
            "Submit a complex task to the multi-tier agent orchestra. "
            "The Senior orchestrator decomposes it, Middle orchestrators assign directions, "
            "Junior orchestrators dispatch to specialist agents (Math, Coder, PR, Analyst, etc.). "
            "Returns a task_id. The result is delivered AUTOMATICALLY to the user's chat "
            "when the task finishes — no polling needed if notify_target is provided. "
            "USE THIS TOOL whenever the user message starts with 'Задача для оркестра:' "
            "(or similar phrasing like 'Orchestra task:'). The text AFTER the prefix is the task."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "task": {"type": "string",
                         "description": "The task text (without the trigger prefix)"},
                "notify_target": {
                    "type": "string",
                    "description": (
                        "Optional push-notification target. Format: 'tg:<chat_id>' for Telegram, "
                        "'slack:<channel>', 'whatsapp:<phone>', 'webhook:<url>'. "
                        "If user is in a TG chat, set this to 'tg:<current_chat_id>' so they "
                        "automatically receive the result when done. If omitted, Hermes will "
                        "try to extract chat_id from context."
                    ),
                },
            },
            "required": ["task"],
        },
    },
    "orchestra_tasks_active": {
        "name": "orchestra_tasks_active",
        "description": (
            "List currently running orchestra tasks (status NOT done/dlq/failed). "
            "Use when user asks 'what's being processed', 'show active tasks', "
            "'какие задачи сейчас выполняются'."
        ),
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
    "orchestra_tasks_by_date": {
        "name": "orchestra_tasks_by_date",
        "description": (
            "List all orchestra tasks created on a given date. "
            "Use when user asks for tasks 'за сегодня', 'за вчера', "
            "'за 2026-05-29' or similar."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "date": {
                    "type": "string",
                    "description": "Date in YYYY-MM-DD, or 'today'/'yesterday' shortcuts",
                },
            },
            "required": ["date"],
        },
    },
    "orchestra_status": {
        "name": "orchestra_status",
        "description": "Show orchestra system status: Redis health, active agent count, worker processes.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
    "orchestra_result": {
        "name": "orchestra_result",
        "description": "Fetch the assembled result for a previously submitted task.",
        "parameters": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string", "description": "Task ID returned by orchestra_submit"},
            },
            "required": ["task_id"],
        },
    },
    "orchestra_agents": {
        "name": "orchestra_agents",
        "description": "List all active domain agents with their role, direction, task count, and latency.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
    "orchestra_metrics": {
        "name": "orchestra_metrics",
        "description": "Show per-direction latency metrics and autoscaling thresholds.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
    "orchestra_agent_info": {
        "name": "orchestra_agent_info",
        "description": "Get the full soul, skill, and metrics for a specific agent.",
        "parameters": {
            "type": "object",
            "properties": {
                "agent_id": {"type": "string", "description": "Agent ID (e.g. 'math_a3f92b1c')"},
            },
            "required": ["agent_id"],
        },
    },
    "orchestra_add_agent": {
        "name": "orchestra_add_agent",
        "description": (
            "Dynamically add a new domain agent to the orchestra with a custom soul and skill. "
            "The orchestra starts routing tasks to this agent immediately."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "direction": {"type": "string", "description": "Domain name (e.g. 'legal', 'design', 'security')"},
                "soul":      {"type": "string", "description": "Agent personality/soul description"},
                "skill":     {"type": "string", "description": "Agent skill description"},
                "values":    {"type": "string", "description": "Comma-separated core values (optional)"},
            },
            "required": ["direction", "soul", "skill"],
        },
    },
}

HANDLERS = {
    "orchestra_submit":         handle_orchestra_submit,
    "orchestra_status":         handle_orchestra_status,
    "orchestra_result":         handle_orchestra_result,
    "orchestra_agents":         handle_orchestra_agents,
    "orchestra_metrics":        handle_orchestra_metrics,
    "orchestra_agent_info":     handle_orchestra_agent_info,
    "orchestra_add_agent":      handle_orchestra_add_agent,
    "orchestra_tasks_active":   handle_orchestra_tasks_active,
    "orchestra_tasks_by_date":  handle_orchestra_tasks_by_date,
}

# ---------------------------------------------------------------------------
# Hermes tool registration
# ---------------------------------------------------------------------------

try:
    from tools.registry import registry  # type: ignore

    for name, schema in SCHEMAS.items():
        registry.register(
            name=name,
            toolset="orchestra",
            schema=schema,
            handler=lambda a, _n=name, **kw: HANDLERS[_n](a, **kw),
            check_fn=_check_orchestra_requirements,
        )
    log.debug("Orchestra tools registered (%d tools)", len(SCHEMAS))

except ImportError:
    # Running outside Hermes — tools are still usable via orchestra_ctl.py directly
    pass
