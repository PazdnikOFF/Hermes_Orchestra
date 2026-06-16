"""
modules/llm_bridge.py — все LLM-вызовы оркестра.

Единственное место где система делает вызовы к языковым моделям.
Принимает AgentState, формирует system prompt из soul+skill,
выполняет вызов и возвращает результат.

Поддерживаемые провайдеры:
  - OpenAI (и любой OpenAI-совместимый endpoint — Ollama, LM Studio и т.д.)
  - Anthropic (нативный SDK)
  - Автоопределение по имени модели: claude* → Anthropic, остальное → OpenAI-compat

ПРАВКИ ДЛЯ СЛЕДУЮЩЕЙ ИТЕРАЦИИ:
  - Добавить новый провайдер → добавь метод _call_<provider> и условие в call_llm()
  - Изменить формат system prompt → редактируй _build_agent_system()
  - Добавить streaming → добавь параметр stream=True в call_llm() и соответствующие методы
  - Добавить retry логику → оберни вызов в tenacity.retry
"""

from __future__ import annotations

import json
import logging
import time
from typing import Optional

from modules.models import AgentState, Skill, Soul
from modules.redis_bus import AgentRegistry
from modules import config as cfg_module

log = logging.getLogger("orchestra.llm_bridge")


# ── Кэш клиентов ──────────────────────────────────────────────────────────
# Создание клиента на каждый вызов = новый httpx-пул + TLS-handshake без
# keep-alive. Кэшируем по (base_url, api_key) — клиент потокобезопасен, а
# каждый воркер живёт в своём процессе, поэтому per-process singleton корректен.

_openai_clients: dict[tuple, object] = {}
_anthropic_clients: dict[str, object] = {}


def _get_openai_client():
    c = cfg_module.CFG
    api_key = c.openai_api_key or "hermes-proxy"
    key = (c.openai_base_url, api_key)
    client = _openai_clients.get(key)
    if client is None:
        from openai import OpenAI  # type: ignore
        # max_retries=5 + timeout=120: устойчиво к переподнятию hermes proxy
        # и редким разрывам OAuth refresh.
        client = OpenAI(
            api_key=api_key,
            base_url=c.openai_base_url,
            max_retries=5,
            timeout=120.0,
        )
        _openai_clients[key] = client
    return client


def _get_anthropic_client():
    key = cfg_module.CFG.anthropic_api_key
    client = _anthropic_clients.get(key)
    if client is None:
        import anthropic  # type: ignore
        client = anthropic.Anthropic(api_key=key)
        _anthropic_clients[key] = client
    return client


def reset_clients() -> None:
    """Сбрасывает кэш клиентов — вызвать при смене конфигурации в рантайме."""
    _openai_clients.clear()
    _anthropic_clients.clear()


# ── Низкоуровневый вызов ──────────────────────────────────────────────────

def call_llm(
    system: str,
    messages: list[dict],
    model: Optional[str] = None,
    max_tokens: int = 2048,
) -> str:
    """Маршрутизирует вызов к нужному провайдеру по имени модели."""
    m = model or cfg_module.CFG.model
    if m.startswith("claude"):
        return _call_anthropic(system, messages, m, max_tokens)
    return _call_openai_compat(system, messages, m, max_tokens)


def _call_openai_compat(system: str, messages: list[dict], model: str, max_tokens: int) -> str:
    try:
        client = _get_openai_client()
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "system", "content": system}] + messages,
            max_tokens=max_tokens,
        )
        return resp.choices[0].message.content or ""
    except Exception as exc:
        # Понятная подсказка вместо голого traceback
        hint = ""
        msg = str(exc)
        if "Connection refused" in msg or "APIConnectionError" in msg:
            hint = (
                f" | Hermes-proxy не отвечает на {cfg_module.CFG.openai_base_url}. "
                "Проверь: `pgrep -af 'hermes.*proxy'` и подними если упал: "
                "`nohup hermes proxy start --provider xai > ~/.hermes/proxy.log 2>&1 &`"
            )
        log.error("OpenAI call failed (model=%s, base_url=%s): %s%s",
                  model, cfg_module.CFG.openai_base_url, exc, hint)
        raise


def embed_text(text: str, model: Optional[str] = None) -> list[float]:
    """
    Возвращает embedding-вектор через OpenAI-совместимый endpoint.
    Используется SoulRegistry для cosine-similarity маршрутизации (TODO 4).

    Возвращает пустой список при ошибке — вызывающий код должен это обработать
    и сделать fallback на keyword-overlap.
    """
    try:
        client = _get_openai_client()
        m = model or cfg_module.CFG.embedding_model
        resp = client.embeddings.create(model=m, input=text[:8000])
        return list(resp.data[0].embedding)
    except Exception as exc:
        log.warning("embed_text failed (%s): %s", model, exc)
        return []


def _call_anthropic(system: str, messages: list[dict], model: str, max_tokens: int) -> str:
    try:
        client = _get_anthropic_client()
        resp = client.messages.create(
            model=model,
            system=system,
            messages=messages,
            max_tokens=max_tokens,
        )
        return resp.content[0].text
    except Exception as exc:
        log.error("Anthropic call failed (%s): %s", model, exc)
        raise


# ── Агентские вызовы ──────────────────────────────────────────────────────

def agent_call(
    agent: AgentState,
    task_content: str,
    extra_context: str = "",
    workspace_id: Optional[str] = None,
) -> str:
    """
    Выполняет задачу через агента. Инжектирует soul+skill в system prompt.
    Возвращает сырой текст ответа LLM.

    Если включены инструменты (CFG.enable_agent_tools) и модель openai-совместимая
    (Grok) — запускается tool-loop: агент реально фетчит URL и пишет файлы проекта
    в workspace задачи. При любом сбое — graceful fallback на обычную генерацию.
    """
    c = cfg_module.CFG
    use_tools = (
        getattr(c, "enable_agent_tools", True)
        and not (c.model or "").startswith("claude")   # tool-loop реализован для Grok/openai-compat
        and bool(workspace_id)
    )
    log.info("[%s/%s] вызов LLM%s (%.60s…)",
             agent.direction, agent.id, " +tools" if use_tools else "", task_content)
    if use_tools:
        try:
            return _agent_call_with_tools(agent, task_content, extra_context, workspace_id)
        except Exception as exc:
            log.warning("[%s] tool-loop сбой (%s) → fallback на текст", agent.id, exc)

    system = _build_agent_system(agent, extra_context)
    return call_llm(system, [{"role": "user", "content": task_content}])


def _agent_call_with_tools(
    agent: AgentState, task_content: str, extra_context: str, workspace_id: str
) -> str:
    """Tool-loop: модель просит инструменты, хост их исполняет и возвращает результат."""
    from modules.agent_tools import ensure_workspace, execute_tool, TOOL_SPECS

    workspace = ensure_workspace(workspace_id)
    system    = _build_agent_system(agent, extra_context, workspace=str(workspace))
    client    = _get_openai_client()
    model     = cfg_module.CFG.model
    max_iters = getattr(cfg_module.CFG, "agent_tool_max_iters", 12)

    messages: list[dict] = [
        {"role": "system", "content": system},
        {"role": "user",   "content": task_content},
    ]

    for _ in range(max_iters):
        resp = client.chat.completions.create(
            model=model, messages=messages, tools=TOOL_SPECS, max_tokens=4096,
        )
        msg = resp.choices[0].message
        tool_calls = msg.tool_calls or []

        assistant_msg: dict = {"role": "assistant", "content": msg.content or ""}
        if tool_calls:
            assistant_msg["tool_calls"] = [
                {"id": tc.id, "type": "function",
                 "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
                for tc in tool_calls
            ]
        messages.append(assistant_msg)

        if not tool_calls:
            return msg.content or ""

        for tc in tool_calls:
            try:
                args = json.loads(tc.function.arguments or "{}")
            except Exception:
                args = {}
            result = execute_tool(tc.function.name, args, workspace)
            log.info("[%s] tool %s(%.60s) → %d симв.",
                     agent.id, tc.function.name, str(args), len(result))
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": result})

    # Исчерпали лимит итераций — просим финальный ответ уже без инструментов.
    resp = client.chat.completions.create(model=model, messages=messages, max_tokens=4096)
    return resp.choices[0].message.content or ""


def _build_agent_system(agent: AgentState, extra_context: str = "",
                        workspace: Optional[str] = None) -> str:
    """Собирает system prompt: soul + skill + (инструменты) + формат вывода."""
    parts = [
        agent.soul.to_prompt(),
        agent.skill.to_prompt(),
    ]
    if extra_context:
        parts.append(f"Дополнительный контекст: {extra_context}")
    if workspace:
        parts.append(
            "У тебя есть ИНСТРУМЕНТЫ — вызывай их, а не описывай словами:\n"
            "  • http_fetch(url) — скачать реальные данные из интернета;\n"
            "  • write_file(path, content), read_file(path), list_files() —\n"
            f"    файлы в твоём рабочем каталоге проекта: {workspace}\n"
            "Делай задачу ПО-НАСТОЯЩЕМУ: если нужны данные/структура страницы — "
            "СКАЧАЙ через http_fetch (не выдумывай). Если нужен код/конфиги/Docker — "
            "СОЗДАЙ реальные файлы через write_file. В финальном 'result' кратко "
            "опиши, что сделал, и перечисли созданные файлы (через list_files)."
        )
    parts.append(
        "Когда задача выполнена, ответь JSON-объектом с ключами:\n"
        '  "result"  — твой основной вывод (строка)\n'
        '  "notes"   — краткие заметки по подходу (строка, необязательно)\n'
        '  "learned" — одно предложение о том, что ты узнал или новый паттерн (строка, необязательно)\n'
        "Только JSON. Без markdown-обрамления."
    )
    return "\n\n".join(parts)


def parse_agent_output(raw: str) -> dict:
    """Парсит JSON-ответ агента, обрабатывает некорректные форматы."""
    raw = raw.strip()
    if raw.startswith("```"):
        raw = "\n".join(l for l in raw.split("\n") if not l.startswith("```"))
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"result": raw, "notes": "ответ не является валидным JSON", "learned": None}


# ── Эволюция скилла ───────────────────────────────────────────────────────

def maybe_evolve_skill(agent: AgentState, learned: Optional[str]) -> None:
    """
    Логирует learned-запись и каждые N задач обновляет описание скилла.
    N задаётся через CFG.skill_evolve_every.
    """
    if not learned:
        return

    AgentRegistry.append_skill_history(agent.id, {
        "ts":           time.time(),
        "task_count":   agent.tasks_completed,
        "learned":      learned,
    })

    if agent.tasks_completed % cfg_module.CFG.skill_evolve_every != 0:
        return

    history = AgentRegistry.get_skill_history(agent.id, last_n=cfg_module.CFG.skill_evolve_every)
    if not history:
        return

    learned_lines = "\n".join(f"- {h['learned']}" for h in history if h.get("learned"))
    prompt = (
        f"Текущее описание скилла (v{agent.skill.version}):\n{agent.skill.description}\n\n"
        f"Последние {cfg_module.CFG.skill_evolve_every} усвоенных паттернов:\n{learned_lines}\n\n"
        "Напиши ОБНОВЛЁННОЕ описание скилла, включающее эти паттерны. "
        "Не более 3 предложений. Только текст описания."
    )
    try:
        new_desc = call_llm(
            "Ты куратор скиллов. Обновляй описания кратко и точно.",
            [{"role": "user", "content": prompt}],
            max_tokens=200,
        ).strip().strip('"\'')

        if new_desc:
            agent.skill = Skill(
                version=agent.skill.version + 1,
                description=new_desc,
                strengths=agent.skill.strengths,
                learned_patterns=agent.skill.learned_patterns + [learned],
                last_updated=time.time(),
            )
            AgentRegistry.save(agent)
            log.info("[%s] Скилл эволюционировал до v%d", agent.id, agent.skill.version)
    except Exception as exc:
        log.warning("[%s] Эволюция скилла не удалась: %s", agent.id, exc)


# ── Оркестраторские вызовы ────────────────────────────────────────────────

def senior_plan_task(
    task_content: str,
    available_directions: list[str] | None = None,
) -> list[dict]:
    """
    Senior принимает решение: какие направления нужны и в каком количестве.
    Оценивает задачу ОБЪЕКТИВНО — без ограничений по тому, что есть в системе.
    Если available_directions передан — помечает направления как "есть" / "нет",
    но это не ограничивает вывод LLM.

    Возвращает список:
    [{"direction": str, "subtask": str, "count": int}, ...]
    """
    context = ""
    if available_directions:
        context = (
            f"\nСправка (что уже есть в системе): {', '.join(available_directions)}\n"
            "Используй эти направления если подходят, но не ограничивайся ими — "
            "называй нужные направления даже если их ещё нет.\n"
        )
    system = (
        "Ты — Старший Оркестратор. Твоя задача — объективно декомпозировать "
        "задачу на направления которые нужны для её решения.\n"
        "Оценивай задачу независимо от того, какие агенты уже есть в системе.\n"
        "Называй точные направления: math, coder, writer, pr, analyst, researcher "
        "или любое другое конкретное направление если задача этого требует.\n"
        f"{context}\n"
        "Ответь JSON-массивом объектов:\n"
        '  "direction"  — название направления (snake_case, на английском)\n'
        '  "subtask"    — конкретная инструкция для этого направления\n'
        '  "count"      — сколько параллельных оркестраторов создать (обычно 1)\n'
        '  "depends_on" — массив направлений, результаты которых нужны ДО старта\n'
        "                 этого направления (опционально). Используй для\n"
        "                 ПОСЛЕДОВАТЕЛЬНЫХ задач: например writer зависит от\n"
        "                 researcher и analyst — он получит их результаты на вход.\n"
        "                 Независимые направления НЕ указывай в depends_on —\n"
        "                 они выполнятся параллельно. Не создавай циклов.\n"
        "Только JSON-массив."
    )
    raw = call_llm(system, [{"role": "user", "content": task_content}], max_tokens=1024)
    raw = raw.strip()
    if raw.startswith("```"):
        raw = "\n".join(l for l in raw.split("\n") if not l.startswith("```"))
    try:
        result = json.loads(raw)
        if isinstance(result, list):
            return result
    except Exception:
        log.warning("Senior plan parse failed, fallback")
    return [{"direction": "researcher", "subtask": task_content, "count": 1}]


def middle_plan_subtask(direction: str, subtask: str) -> list[dict]:
    """
    Middle решает декомпозировать или нет.

    Принцип: атомарная задача → одна микрозадача (та же что и subtask).
    Только если задача СОДЕРЖИТ явные независимые подпункты — делим.
    Перерасщепление приводит к фрагментарным результатам которые Judge
    закономерно браковает.

    Эвристика на стороне Python: короткий subtask → не зовём LLM вообще.
    """
    # Атомарные/короткие задачи — не дробим, прямой проход к Agent
    if len(subtask) < 200:
        return [{"micro_task": subtask}]

    system = (
        f"Ты — Средний Оркестратор по направлению '{direction}'.\n\n"
        "Реши, нужна ли декомпозиция:\n"
        "  - Если задача АТОМАРНА (один deliverable, один агент справится за один проход) — "
        "верни массив из ОДНОГО элемента: исходный subtask без изменений.\n"
        "  - Только если задача содержит ЯВНЫЕ независимые подпункты "
        "(каждый — свой deliverable, не зависит от других) — раздели на 2–4 микрозадачи.\n\n"
        "НЕ дроби монолитную задачу на шаги реализации (signature → loop → return) — "
        "это даёт фрагменты, которые исполнитель не сможет завершить отдельно.\n\n"
        "Ответь JSON-массивом: [{\"micro_task\": \"...\"}, ...]\nТолько JSON."
    )
    raw = call_llm(system, [{"role": "user", "content": subtask}], max_tokens=512)
    raw = raw.strip()
    if raw.startswith("```"):
        raw = "\n".join(l for l in raw.split("\n") if not l.startswith("```"))
    try:
        result = json.loads(raw)
        if isinstance(result, list) and result:
            return result
    except Exception:
        pass
    return [{"micro_task": subtask}]


def judge_evaluate(direction: str, task: str, output: str, soul_prompt: str) -> dict:
    """
    Judge оценивает результат агента.

    Возвращает:
      score             — float 0.0..1.0 (общее качество)
      passed            — bool (выводится из score, но LLM также подтверждает)
      verdict           — 1-2 предложения общей оценки
      critique          — конкретные actionable пункты что улучшить (для rework)
      needs_doubt_agent — bool, нужна ли глубокая проверка скептиком
      doubt_focus       — что именно проверить агенту-скептику
    """
    system = (
        "Ты — Судья. Оценивай результаты агентов по критериям: "
        "полнота, корректность, отсутствие галлюцинаций, соответствие задаче.\n\n"
        "ЖЁСТКИЕ ПРАВИЛА (приоритетнее шкалы ниже):\n"
        "  1. РЕЛЕВАНТНОСТЬ. Если результат отвечает на ДРУГУЮ тему/предметную область, "
        "чем задача (задача про X — ответ про Y) — score ≤ 0.2 НЕЗАВИСИМО от того, "
        "насколько он хорош сам по себе. В critique прямо укажи, что тема не соответствует.\n"
        "  2. НЕПОДКРЕПЛЁННЫЕ ФАКТЫ. Если результат приводит конкретные числа, статистику, "
        "даты, цитаты или факты, которых НЕТ в условии задачи и которые исполнитель не мог "
        "знать достоверно (у него нет доступа к интернету/данным) — считай их потенциально "
        "выдуманными: снизь score и в critique потребуй пометить их как «оценка, без источника» "
        "либо убрать. Уверенные точные цифры без источника — признак галлюцинации.\n\n"
        "Шкала score:\n"
        "  0.0–0.5 — серьёзные проблемы, результат не пригоден\n"
        "  0.5–0.79 — есть существенные недочёты, требуется доработка\n"
        "  0.8–0.9 — хороший результат, мелкие улучшения возможны\n"
        "  0.9–1.0 — отличный результат\n\n"
        "Если score <= 0.79 — обязательно дай конкретную actionable критику в поле critique:\n"
        "что именно неверно, чего не хватает, как исправить. Пиши так, чтобы агент мог\n"
        "по этому тексту сразу переделать результат.\n\n"
        "Ответь JSON:\n"
        '  "score"             — float 0.0..1.0\n'
        '  "passed"            — bool (true если score > 0.79)\n'
        '  "verdict"           — 1-2 предложения общей оценки\n'
        '  "critique"          — что улучшить (обязательно при score<=0.79, иначе "")\n'
        '  "needs_doubt_agent" — bool\n'
        '  "doubt_focus"       — что проверить агенту-скептику\n'
        "Только JSON."
    )
    content = (
        f"Направление: {direction}\nЗадача: {task}\n"
        f"Душа агента: {soul_prompt}\n\nРезультат:\n{output}"
    )
    raw = call_llm(system, [{"role": "user", "content": content}], max_tokens=600)
    raw = raw.strip()
    if raw.startswith("```"):
        raw = "\n".join(l for l in raw.split("\n") if not l.startswith("```"))
    try:
        parsed = json.loads(raw)
    except Exception:
        return {
            "score": 1.0, "passed": True, "verdict": "parse error — пропускаем",
            "critique": "", "needs_doubt_agent": False, "doubt_focus": "",
        }
    # Нормализация: гарантируем поля и тип
    try:
        parsed["score"] = max(0.0, min(1.0, float(parsed.get("score", 0.0))))
    except (TypeError, ValueError):
        parsed["score"] = 0.0
    parsed.setdefault("passed", parsed["score"] > 0.79)
    parsed.setdefault("verdict", "")
    parsed.setdefault("critique", "")
    parsed.setdefault("needs_doubt_agent", False)
    parsed.setdefault("doubt_focus", "")
    return parsed


def doubt_agent_review(
    direction: str,
    task: str,
    output: str,
    doubt_focus: str,
    doubt_overlay: str,
) -> str:
    """Эфемерный агент-скептик. Возвращает текст критики."""
    system = (
        f"Ты — специалист по направлению {direction}, но в роли критика.\n"
        f"{doubt_overlay}\n\n"
        "Напиши конкретную, actionable критику. "
        "Укажи конкретные проблемы. НЕ подтверждай правильность."
    )
    content = f"Задача: {task}\nФокус проверки: {doubt_focus}\n\nРезультат:\n{output}"
    return call_llm(system, [{"role": "user", "content": content}], max_tokens=512)


def assemble_final_result(task_content: str, direction_results: dict[str, list[str]]) -> str:
    """Senior собирает финальный ответ из результатов всех направлений."""
    system = (
        "Ты — Старший Оркестратор. Собери параллельные результаты в единый "
        "связный ответ. Сохрани важные детали каждого направления. Убери дубликаты."
    )
    parts = [f"Исходная задача: {task_content}\n\nРезультаты по направлениям:"]
    for direction, results in direction_results.items():
        parts.append(f"\n### {direction.upper()}")
        for i, r in enumerate(results, 1):
            parts.append(f"{i}. {r}")
    return call_llm(system, [{"role": "user", "content": "\n".join(parts)}], max_tokens=4096)
