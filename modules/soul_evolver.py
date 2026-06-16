"""
modules/soul_evolver.py — обратная запись улучшений в YAML-файлы душ.

Когда Judge подтверждает что результат агента корректен И есть потенциал
для улучшения, SoulEvolver:
  1. Через LLM определяет что именно стоит улучшить в душе/скилле
  2. Записывает обновлённый soul.yaml обратно в SOULs/
  3. Обновляет индекс в SoulRegistry
  4. Сохраняет историю изменений для возможности rollback

ПРИНЦИПЫ:
  - Улучшения записываются ТОЛЬКО при verdict.passed=True
  - Изменения аддитивные: новые strengths/patterns добавляются, старые не удаляются
  - personality меняется осторожно — только уточнение, не переписывание
  - Каждая версия сохраняется в git-стиле (в YAML с историей)

КЛЮЧИ REDIS:
  soul_evolution:{direction}        List   — лог изменений (JSON)
  soul_evolution:lock:{direction}   String — lock на время записи (5 сек)

ПРАВКИ ДЛЯ СЛЕДУЮЩЕЙ ИТЕРАЦИИ:
  - Изменить что именно улучшается → редактируй _decide_improvements()
  - Добавить rollback → читай soul_evolution:{direction}, откати soul.yaml
  - Изменить порог улучшения → редактируй IMPROVE_SCORE_THRESHOLD
  - Добавить уведомление пользователя при эволюции → добавь в _apply_improvements()
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Optional

import yaml

from modules.models import Soul, Skill
from modules.redis_bus import get_redis

log = logging.getLogger("orchestra.soul_evolver")

# Порог вердикта Judge при котором стоит улучшать душу
# (0.0 = улучшаем при любом passed, 0.7 = только при высокой оценке)
IMPROVE_SCORE_THRESHOLD = 0.0
EVOLUTION_LOG_MAX = 50  # максимум записей в истории


class SoulEvolver:
    """
    Записывает улучшения обратно в soul.yaml после одобрения Judge.
    """

    def __init__(self, souls_dir: Path, soul_registry):
        self.souls_dir     = souls_dir
        self.soul_registry = soul_registry

    # ── Главный метод ─────────────────────────────────────────────────────

    def maybe_evolve(
        self,
        direction: str,
        role_type: str,        # "agent" | "orchestrator"
        task_content: str,
        result_text: str,
        learned: Optional[str],
        verdict: dict,         # от JudgeWorker: {passed, verdict, critique}
    ) -> bool:
        """
        Оценивает нужно ли улучшать душу. Если да — улучшает и записывает в YAML.
        Возвращает True если улучшение было применено.
        """
        # Улучшаем только когда Judge одобрил
        if not verdict.get("passed", False):
            return False

        # Нет чему учиться — пропускаем
        if not learned and not verdict.get("verdict"):
            return False

        # Определяем что улучшить
        improvements = self._decide_improvements(
            direction, role_type, task_content, result_text, learned, verdict
        )
        if not improvements:
            return False

        # Применяем с защитой от гонки
        return self._apply_with_lock(direction, role_type, improvements, learned, task_content)

    # ── Анализ улучшений ──────────────────────────────────────────────────

    def _decide_improvements(
        self,
        direction: str,
        role_type: str,
        task: str,
        result: str,
        learned: Optional[str],
        verdict: dict,
    ) -> Optional[dict]:
        """
        LLM анализирует результат и вердикт, решает что улучшить в душе/скилле.
        Возвращает dict с полями improvements или None если улучшений нет.
        """
        from modules.llm_bridge import call_llm

        soul_path = self._soul_path(direction, role_type)
        if not soul_path.exists():
            return None

        current = yaml.safe_load(soul_path.read_text(encoding="utf-8")) or {}
        soul_data  = current.get("soul", {})
        skill_data = current.get("skill", {})

        system = (
            "Ты эволюционный куратор AI-агентов. "
            "Анализируешь выполненную задачу и решаешь как улучшить душу и скилл агента. "
            "Улучшения должны быть точечными, аддитивными и основанными на фактах из задачи. "
            "Не выдумывай — опирайся только на то что реально проявилось в работе."
        )
        prompt = (
            f"Направление агента: {direction}\n"
            f"Текущий скилл: {skill_data.get('description','')[:200]}\n"
            f"Текущие сильные стороны: {skill_data.get('strengths', [])}\n\n"
            f"Выполненная задача: {task[:300]}\n"
            f"Результат агента: {result[:400]}\n"
            f"Вердикт Judge: {verdict.get('verdict','')}\n"
            f"Усвоенный паттерн: {learned or 'нет'}\n\n"
            "Определи улучшения. Ответь JSON:\n"
            '  "has_improvements": bool\n'
            '  "new_strength": строка или null — новая сильная сторона (если проявилась)\n'
            '  "new_pattern": строка или null — новый паттерн (если выучен)\n'
            '  "skill_refinement": строка или null — уточнение описания скилла\n'
            '  "personality_note": строка или null — уточнение личности (только если критично)\n'
            "Будь консервативен: personality меняй только если задача явно выявила новое качество. "
            "Только JSON."
        )
        raw = call_llm(system, [{"role": "user", "content": prompt}], max_tokens=400)
        raw = raw.strip()
        if raw.startswith("```"):
            raw = "\n".join(l for l in raw.split("\n") if not l.startswith("```"))
        try:
            result_d = json.loads(raw)
            if not result_d.get("has_improvements", False):
                return None
            return result_d
        except Exception as exc:
            log.warning("[SoulEvolver] ошибка парсинга ответа: %s", exc)
            return None

    # ── Применение улучшений ──────────────────────────────────────────────

    def _apply_with_lock(
        self,
        direction: str,
        role_type: str,
        improvements: dict,
        learned: Optional[str],
        task: str,
    ) -> bool:
        """Применяет улучшения с Redis-локом для защиты от параллельной записи."""
        r = get_redis()
        lock_key = f"soul_evolution:lock:{direction}"

        # Атомарный lock на 5 секунд
        acquired = r.set(lock_key, "1", nx=True, ex=5)
        if not acquired:
            log.debug("[SoulEvolver] lock занят для %s, пропускаем", direction)
            return False

        try:
            applied = self._apply_improvements(direction, role_type, improvements)
            if applied:
                self._log_evolution(direction, improvements, learned, task)
                # Обновляем индекс
                soul_path = self._soul_path(direction, role_type)
                if soul_path.exists():
                    self.soul_registry.index_one(soul_path, direction, role_type)
            return applied
        finally:
            r.delete(lock_key)

    def _apply_improvements(
        self, direction: str, role_type: str, improvements: dict
    ) -> bool:
        """Читает soul.yaml, применяет изменения, записывает обратно."""
        soul_path = self._soul_path(direction, role_type)
        if not soul_path.exists():
            log.warning("[SoulEvolver] %s не найден", soul_path)
            return False

        raw_text = soul_path.read_text(encoding="utf-8")
        data = yaml.safe_load(raw_text) or {}
        changed = False

        soul_data  = data.setdefault("soul", {})
        skill_data = data.setdefault("skill", {})

        # Новая сильная сторона
        new_strength = improvements.get("new_strength")
        if new_strength:
            strengths = skill_data.setdefault("strengths", [])
            if new_strength not in strengths:
                strengths.append(new_strength)
                changed = True
                log.info("[SoulEvolver] %s ← новая сила: %s", direction, new_strength)

        # Новый паттерн
        new_pattern = improvements.get("new_pattern")
        if new_pattern:
            patterns = skill_data.setdefault("learned_patterns", [])
            if new_pattern not in patterns:
                patterns.append(new_pattern)
                # Держим не более 20 паттернов в YAML
                if len(patterns) > 20:
                    patterns.pop(0)
                changed = True
                log.info("[SoulEvolver] %s ← новый паттерн: %s", direction, new_pattern)

        # Уточнение скилла
        skill_ref = improvements.get("skill_refinement")
        if skill_ref:
            old_desc = skill_data.get("description", "")
            # Добавляем уточнение в конец если оно новое
            if skill_ref not in old_desc:
                skill_data["description"] = old_desc.rstrip(". ") + ". " + skill_ref
                skill_data["version"] = int(skill_data.get("version", 1)) + 1
                changed = True
                log.info("[SoulEvolver] %s ← скилл v%d уточнён",
                         direction, skill_data["version"])

        # Уточнение личности (очень осторожно)
        personality_note = improvements.get("personality_note")
        if personality_note:
            old_p = soul_data.get("personality", "")
            # Только если note принципиально новый (не подстрока)
            if personality_note not in old_p and len(personality_note) > 10:
                soul_data["personality"] = old_p.rstrip(" ") + " " + personality_note
                changed = True
                log.info("[SoulEvolver] %s ← личность уточнена", direction)

        if changed:
            # Сохраняем с заголовком
            header_lines = [l for l in raw_text.split("\n") if l.startswith("#")]
            header = "\n".join(header_lines) + "\n\n" if header_lines else ""
            soul_path.write_text(
                header + yaml.dump(data, allow_unicode=True, default_flow_style=False),
                encoding="utf-8",
            )

        return changed

    # ── История изменений ─────────────────────────────────────────────────

    def _log_evolution(
        self,
        direction: str,
        improvements: dict,
        learned: Optional[str],
        task: str,
    ) -> None:
        r = get_redis()
        entry = {
            "ts":           time.time(),
            "direction":    direction,
            "improvements": improvements,
            "learned":      learned,
            "task_preview": task[:100],
        }
        key = f"soul_evolution:{direction}"
        r.rpush(key, json.dumps(entry))
        # Обрезаем историю
        r.ltrim(key, -EVOLUTION_LOG_MAX, -1)

    def get_evolution_log(self, direction: str, last_n: int = 10) -> list[dict]:
        """Возвращает последние N записей истории эволюции для direction."""
        raw = get_redis().lrange(f"soul_evolution:{direction}", -last_n, -1)
        return [json.loads(x) for x in raw]

    # ── Вспомогательные ───────────────────────────────────────────────────

    def _soul_path(self, direction: str, role_type: str) -> Path:
        if role_type == "agent":
            return self.souls_dir / "agents" / direction / "soul.yaml"
        return self.souls_dir / "orchestrators" / direction / "soul.yaml"
