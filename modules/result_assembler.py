"""
modules/result_assembler.py — авто-сборка финального результата задачи.

Когда Senior декомпозирует задачу пользователя на subtasks, каждый из них
обрабатывается отдельным Middle/Junior/Agent и оценивается Judge.
Этот модуль отслеживает готовность всех subtasks одной корневой задачи
и собирает финальный ответ через assemble_final_result().

КЛЮЧИ REDIS:
  results:{parent_task_id}                Hash   — корневая задача
    decomposition                          JSON   — {direction: [subtask_id, ...]}
    original_task                          str    — текст задачи пользователя
    status                                 str    — partial | decomposed | done | dlq | waiting_for_souls
    final_result                           str    — финальный ответ (после assemble)
    assembled_at                           str    — timestamp сборки
  results:assemble_lock:{parent_task_id}  String — короткий lock (10 сек)

КАК РАБОТАЕТ:
  1. SeniorWorker._handle() сохраняет decomposition_map и original_task
  2. После каждого save_result(..., "status", "done") в JudgeWorker
     вызывается ResultAssembler.maybe_assemble(parent_task_id)
  3. ResultAssembler: проверяет статусы всех сабтасков из decomposition,
     если все DONE или DLQ — берёт result из results:{subtask_id},
     группирует по direction, вызывает llm_bridge.assemble_final_result
  4. Финальный ответ записывается в results:{parent_task_id}.final_result
  5. Динамические оркестраторы Middle/Junior удаляются (cleanup)

ИДЕМПОТЕНТНОСТЬ:
  - Если final_result уже записан — повторно не собираем
  - Lock защищает от двойной сборки при гонке Judge-воркеров

ПРАВКИ ДЛЯ СЛЕДУЮЩЕЙ ИТЕРАЦИИ:
  - Изменить формат сборки → llm_bridge.assemble_final_result
  - Изменить cleanup → factory.cleanup_dynamic_orchestrators
  - Добавить уведомление пользователя по готовности → расширь _finalize
"""

from __future__ import annotations

import json
import logging
import time
from typing import Optional

from modules.redis_bus import get_redis, load_result, save_result

log = logging.getLogger("orchestra.result_assembler")

ASSEMBLE_LOCK_TTL = 10  # секунд
TERMINAL_STATUSES = {"done", "dlq", "failed"}


class ResultAssembler:
    """Собирает финальный результат когда все сабтаски готовы."""

    def __init__(self, factory=None):
        # factory нужен только для cleanup; передаётся опционально
        self.factory = factory

    def maybe_assemble(self, parent_task_id: Optional[str]) -> bool:
        """
        Триггерится из JudgeWorker после каждого done-сабтаска.
        Возвращает True если сборка состоялась.
        """
        if not parent_task_id:
            return False

        data = load_result(parent_task_id)
        if not data:
            return False

        # Уже собрано — не дублируем
        if data.get("final_result"):
            return False

        decomp_raw = data.get("decomposition")
        if not decomp_raw:
            return False

        try:
            decomposition: dict[str, list[str]] = json.loads(decomp_raw)
        except json.JSONDecodeError:
            log.warning("[Assembler] невалидный decomposition для %s", parent_task_id)
            return False

        # Если статус был waiting_for_souls — сборка не имеет смысла, ждём gap-resolve
        if data.get("status") == "waiting_for_souls":
            return False

        all_subtask_ids = [tid for ids in decomposition.values() for tid in ids]
        if not all_subtask_ids:
            return False

        # Проверяем готовность всех
        subtask_results = self._collect_subtask_statuses(all_subtask_ids)
        not_terminal = [
            sid for sid, st in subtask_results.items()
            if st.get("status") not in TERMINAL_STATUSES
        ]
        if not_terminal:
            log.debug("[Assembler] %s: ждём %d/%d сабтасков",
                      parent_task_id, len(not_terminal), len(all_subtask_ids))
            return False

        # Атомарный lock — защита от гонки воркеров
        r = get_redis()
        lock_key = f"results:assemble_lock:{parent_task_id}"
        if not r.set(lock_key, "1", nx=True, ex=ASSEMBLE_LOCK_TTL):
            log.debug("[Assembler] lock занят для %s", parent_task_id)
            return False

        try:
            # Повторно читаем после lock — другой воркер мог успеть
            recheck = load_result(parent_task_id)
            if recheck.get("final_result"):
                return False

            return self._finalize(parent_task_id, data, decomposition, subtask_results)
        finally:
            r.delete(lock_key)

    # ── Внутреннее ────────────────────────────────────────────────────────

    def _collect_subtask_statuses(self, subtask_ids: list[str]) -> dict[str, dict]:
        """Получает результаты всех сабтасков одним батчем."""
        r = get_redis()
        pipe = r.pipeline()
        for sid in subtask_ids:
            pipe.hgetall(f"results:{sid}")
        raw = pipe.execute()
        return dict(zip(subtask_ids, raw))

    def _finalize(
        self,
        parent_task_id: str,
        parent_data: dict,
        decomposition: dict[str, list[str]],
        subtask_results: dict[str, dict],
    ) -> bool:
        from modules.llm_bridge import assemble_final_result

        original = parent_data.get("original_task", "")

        # Группируем результаты по direction
        direction_results: dict[str, list[str]] = {}
        had_dlq = False
        for direction, ids in decomposition.items():
            collected = []
            for sid in ids:
                sub = subtask_results.get(sid, {})
                if sub.get("status") == "dlq":
                    had_dlq = True
                    collected.append(f"[DLQ] {sub.get('last_error','')}")
                elif sub.get("result"):
                    collected.append(sub["result"])
            if collected:
                direction_results[direction] = collected

        if not direction_results:
            log.warning("[Assembler] %s: нет полезных результатов для сборки",
                        parent_task_id)
            save_result(parent_task_id, "status", "failed")
            save_result(parent_task_id, "final_result",
                        "Все сабтаски завершились без полезных результатов.")
            return True

        # Шорткат: если суммарно ровно один полезный результат и нет DLQ —
        # собирать нечего, LLM-вызов был бы лишним. Возвращаем как есть.
        total_results = sum(len(v) for v in direction_results.values())
        if total_results == 1 and not had_dlq:
            final = next(iter(direction_results.values()))[0]
            log.info("[Assembler] %s — единственный результат, сборка без LLM",
                     parent_task_id)
        else:
            log.info("[Assembler] собираем финальный результат для %s "
                     "(%d направлений, dlq=%s)",
                     parent_task_id, len(direction_results), had_dlq)
            try:
                final = assemble_final_result(original, direction_results)
            except Exception as exc:
                log.exception("[Assembler] ошибка сборки %s: %s", parent_task_id, exc)
                return False

        save_result(parent_task_id, "final_result", final)
        save_result(parent_task_id, "assembled_at", str(time.time()))
        save_result(parent_task_id, "status", "done" if not had_dlq else "partial_dlq")
        log.info("[Assembler] %s — final_result сохранён (%d симв.)",
                 parent_task_id, len(final))

        # Освобождаем direction'ы и убиваем агентов если ни одна задача
        # их больше не использует. Эволюция душ/скиллов уже прошла
        # per-subtask в JudgeWorker — здесь только teardown.
        try:
            from modules.agent_lifecycle import release_directions_for
            torn = release_directions_for(parent_task_id, factory=self.factory)
            if torn:
                log.info("[Assembler] %s — teardown directions: %s",
                         parent_task_id, ", ".join(torn))
        except Exception as exc:
            log.warning("[Assembler] release_directions failed: %s", exc)

        return True
