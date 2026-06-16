"""
modules/dag.py — чистая логика графа зависимостей направлений (итерация 7).

Senior может выдать план с полем depends_on: направление стартует только
после завершения направлений-предков и получает их результаты как контекст
(data → analysis → synthesis вместо изолированного параллельного fan-out).

Здесь ТОЛЬКО чистые функции — без Redis/LLM, легко тестируются.
Оркестрация (публикация, проброс результатов) живёт в workers.py.

ПРАВКИ ДЛЯ СЛЕДУЮЩЕЙ ИТЕРАЦИИ:
  - Изменить правила готовности → ready_directions()
  - Изменить детекцию циклов → is_acyclic()
"""

from __future__ import annotations

import logging

log = logging.getLogger("orchestra.dag")


def normalize_plan(plan: list[dict]) -> list[dict]:
    """
    Приводит план Senior к каноничному виду:
      - гарантирует поля direction / subtask / count / depends_on
      - дедуплицирует направления (оставляет первую спеку на направление)
      - depends_on фильтруется до известных направлений, без self-dep и дублей
    """
    known: set[str] = set()
    ordered: list[str] = []
    for spec in plan:
        d = str(spec.get("direction", "")).strip()
        if d and d not in known:
            known.add(d)
            ordered.append(d)

    seen: set[str] = set()
    norm: list[dict] = []
    for spec in plan:
        d = str(spec.get("direction", "")).strip()
        if not d or d in seen:
            continue  # пропускаем пустые и дубли направлений
        seen.add(d)

        raw_deps = spec.get("depends_on") or []
        if isinstance(raw_deps, str):
            raw_deps = [raw_deps]
        deps: list[str] = []
        for dep in raw_deps:
            dep = str(dep).strip()
            if dep and dep != d and dep in known and dep not in deps:
                deps.append(dep)

        try:
            count = int(spec.get("count", 1) or 1)
        except (TypeError, ValueError):
            count = 1

        norm.append({
            "direction":  d,
            "subtask":    spec.get("subtask", "") or "",
            "count":      max(1, count),
            "depends_on": deps,
        })
    return norm


def has_dependencies(plan: list[dict]) -> bool:
    """True если хоть у одного направления есть depends_on."""
    return any(s.get("depends_on") for s in plan)


def is_acyclic(plan: list[dict]) -> bool:
    """
    True если граф зависимостей ацикличен (существует топологический порядок).
    Алгоритм Кана: пока есть направления, все зависимости которых разрешены —
    разрешаем их. Если в конце разрешены не все → есть цикл.
    """
    deps = {s["direction"]: set(s.get("depends_on", [])) for s in plan}
    resolved: set[str] = set()
    progress = True
    while progress and len(resolved) < len(deps):
        progress = False
        for d, dd in deps.items():
            if d not in resolved and dd <= resolved:
                resolved.add(d)
                progress = True
    return len(resolved) == len(deps)


def ready_directions(
    plan: list[dict],
    done_dirs: set[str],
    published_dirs: set[str],
) -> list[str]:
    """
    Направления, готовые к публикации прямо сейчас:
    ещё не опубликованы И все их зависимости уже завершены.
    """
    done = set(done_dirs)
    pub = set(published_dirs)
    out: list[str] = []
    for s in plan:
        d = s["direction"]
        if d in pub:
            continue
        if set(s.get("depends_on", [])) <= done:
            out.append(d)
    return out
