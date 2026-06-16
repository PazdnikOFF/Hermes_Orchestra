---
name: orchestra
description: Multi-tier agent orchestra for complex tasks. **PRIMARY TRIGGER**: any user message starting with "Задача для оркестра:" (Russian) or "Orchestra task:" (English) MUST be routed to orchestra_submit with the text after the colon. Also trigger when user wants to delegate a non-trivial request to specialised AI agents (Math, Coder, Writer, PR, Analyst, Researcher), check active orchestra tasks ("какие задачи сейчас в оркестре", "show running tasks"), list tasks by date ("задачи за сегодня/вчера"), fetch a specific result, manage orchestra lifecycle, add/inspect agents, or resolve gap-requests. The orchestra delivers results AUTOMATICALLY back to the user's chat when done — no polling required.
---

# Orchestra — Multi-Tier Agent Pipeline

Orchestra is a Redis-backed pipeline of specialised AI agents with souls (YAML personalities), an evolving skill layer, a Judge that scores results and triggers reworks (max 5 iterations, threshold > 0.79), and automatic final-result assembly. Full architecture lives in `ARCHITECTURE.md` next to this file — read it for the deep model before doing anything non-obvious.

## When to use this skill

Use orchestra when ANY of these is true:

- Task is large enough to benefit from parallel direction-specialists (e.g. "research X, code a prototype, write a PR description").
- User explicitly mentions orchestra / оркестр / "agent pipeline" / "send to the orchestra".
- User asks to add/list/inspect agents, manage souls, view DLQ/evolution-log, or resolve gap-requests.
- User wants a quality-gated answer (Judge rework loop) rather than a single LLM call.

**Do NOT** invoke orchestra for trivial one-shot questions you can answer directly — it has cold-start latency (Redis, multiple LLM calls, validation, rework loop) and consumes credits.

## Tools registered by this skill

| Tool | When to call |
|---|---|
| `orchestra_submit(task, notify_target?)` | **Trigger phrases** "Задача для оркестра: ..." / "Orchestra task: ...". Strip the prefix, pass the rest as `task`. ALWAYS pass `notify_target="tg:<chat_id>"` if the user is in Telegram (extract chat_id from Hermes context) so they get the result automatically. |
| `orchestra_tasks_active()` | User asks "what's running in orchestra", "какие задачи в работе", "show me active tasks". |
| `orchestra_tasks_by_date(date)` | "tasks today/yesterday", "задачи за 2026-05-29". Accepts YYYY-MM-DD, `today`, `yesterday`. |
| `orchestra_result(task_id)` | User asks "result of task_X", "show me what came out of task_X". |
| `orchestra_status()` | Redis + agent count + worker process count. Health check. |
| `orchestra_agents()` | List live agents with role/direction/tasks/latency. |
| `orchestra_metrics()` | Per-direction latency. |
| `orchestra_agent_info(agent_id)` | Full soul+skill+metrics for one agent. |
| `orchestra_add_agent(direction, soul, skill, values?)` | User asks to add a custom agent. |

For operations NOT exposed as tools (gap-resolve, dlq-retry, judge-history, evolution-log), shell out to `scripts/orchestra_ctl.py`.

## Mental model (one paragraph)

User submits a task → Senior decomposes by direction (objective evaluation; if a needed soul is missing, parks the task and emits a `gap_request` to be resolved via `orchestra_ctl gap-resolve`) → Middle splits each direction into micro-tasks → Junior round-robins to AgentWorkers → each agent runs its YAML soul as a system prompt → Judge scores the output 0..1; if `score ≤ 0.79` and `judge_iteration < 5` the same task goes back to the same agent with the critique as `rework_context`, otherwise it's finalised (passed or best-of-5) → when every sibling sub-task of a parent is done, `ResultAssembler` calls `assemble_final_result` and writes `final_result` to `results:{task_id}`.

## Typical flows

**TG user message: "Задача для оркестра: <text>"**
1. Extract everything after the colon as the task text.
2. Call `orchestra_submit(task=<text>, notify_target="tg:<chat_id>")` — chat_id from Hermes context.
3. Respond briefly: "📨 Принято, задача `task_XXX` ушла в оркестр. Результат пришлю как только будет готов." Do NOT poll yourself — NotifierWorker will push the final result to the user via `hermes send tg <chat_id> <result>`.

**User asks "что в работе у оркестра?":**
- Call `orchestra_tasks_active()` and forward formatted output.

**User asks "какие задачи делал сегодня?":**
- Call `orchestra_tasks_by_date(date="today")`.

**User asks for result of specific task:**
- Call `orchestra_result(task_id="task_XXX")`.

**Run a task directly (without push), manually fetch:**
```
orchestra_submit(task="...")        # returns task_id
# wait / let Judge loop finish
orchestra_result(task_id="...")     # shows final_result, score, attempts
```

**Inspect the rework history of a sub-task** (when result was finalised as best-of-5 and you want to know why):
```
bash: python <skill>/scripts/orchestra_ctl.py judge-history <subtask_id>
```

**A new direction appeared (gap_request):** notify the user, then either:
- `orchestra_ctl gap-resolve --action create` — LLM auto-generates the missing soul
- `orchestra_ctl gap-resolve --action upload --files ...` — user-supplied soul.yaml
- `orchestra_ctl gap-resolve --action skip` — use closest available
The parked parent task is auto-resumed in Senior once all its gaps clear.

**Dead-letter queue (when retries exhausted):**
```
bash: python <skill>/scripts/orchestra_ctl.py dlq-list
bash: python <skill>/scripts/orchestra_ctl.py dlq-retry <task_id>
```

## Full CLI (orchestra_ctl.py)

Located at `<skill>/scripts/orchestra_ctl.py`. Run with `python` after `cd` to skill dir or by absolute path.

```
bootstrap                              Init Redis + base agents + soul index
start                                  Spawn Senior/Judge/Watchdog + initial AgentWorkers
stop                                   SIGTERM to startup PIDs + dynamic worker PIDs
status                                 Redis + agents + worker process count
submit "<task>"                        Send to STREAM_SENIOR
result <task_id>                       Show status / decomposition / final_result
watch [--task ID]                      Tail STREAM_SENIOR and STREAM_JUDGE_IN
agents                                 List agents
metrics                                Latency per direction
agent-info <id>                        Soul + skill + history
add-agent --direction X --soul Y --skill Z [--values "a,b"] [--force]
reset-skill <id>                       Drop evolved skill back to soul.yaml's
gc                                     Force idle-clone cleanup
flush                                  DANGER: wipe all orchestra Redis keys

# v3
gap-resolve --action create|upload|skip [--files ...]
dlq-list [--count N]
dlq-retry <task_id>
evolution-log <direction> [--n N]
soul-index [--rebuild] [--llm] [--type agent|orchestrator]

# v4
judge-history <task_id>                Show every rework attempt with score/verdict/critique
```

## Environment expected

Orchestra reads ONLY env vars at startup (no separate config file):

```
REDIS_URL=redis://localhost:6379          required
ORCHESTRA_MODEL=grok-4.3                  required — must match Hermes' default
OPENAI_BASE_URL=http://localhost:<port>/v1  point at `hermes proxy start --provider xai`
OPENAI_API_KEY=hermes-proxy               any non-empty string (proxy overrides)
ORCHESTRA_JUDGE_PASS_THRESHOLD=0.79       default; score must exceed to pass
ORCHESTRA_JUDGE_MAX_ITER=5                default; rework cap before best-of
ORCHESTRA_USE_EMBEDDINGS=0                set to 1 only if your provider supports embeddings
```

Anthropic-direct fallback (skip Hermes proxy): set `ANTHROPIC_API_KEY` and use a `claude-*` model — `llm_bridge` will auto-route.

## Files this skill owns

```
SKILL.md                                 ← this file
ARCHITECTURE.md                          ← read FIRST for any non-trivial change
modules/*.py                             ← orchestra core (one responsibility per module)
SOULs/                                   ← YAML soul files; add a folder to add a direction
scripts/orchestra_ctl.py                 ← CLI entrypoint
```

When the user asks to "edit the math agent's personality" → edit `SOULs/agents/math/soul.yaml`. Adding `SOULs/agents/<new>/soul.yaml` is enough to register a new direction — no Python changes needed.

## What NOT to do

- Don't try to embed orchestra calls inside a single LLM turn (it's async, results land in Redis).
- Don't call `flush` without explicit user confirmation — it wipes every running task.
- Don't manually edit `agent:*:state` keys in Redis; mutate through `AgentRegistry` in `modules/redis_bus.py`.
- Don't add `daemon=True` threads for new workers — use `_spawn_worker_subprocess` in `modules/workers.py` so they survive parent death.
