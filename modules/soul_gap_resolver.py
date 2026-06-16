"""
modules/soul_gap_resolver.py — обработка недостающих душ.

Когда оркестратор определяет что для задачи нужна душа которой нет в SOULs/,
этот модуль:
  1. Описывает пользователю что именно нужно и зачем
  2. Предлагает два варианта: создать автоматически (AI) или загрузить вручную
  3. При выборе "создать" — генерирует полноценную душу через LLM на основе
     структуры похожих агентов, но не копирует их
  4. При выборе "загрузить" — пишет для каждой души текст задачи,
     которую должен решать этот агент (контекст для пользователя)
  5. Сохраняет новую душу в SOULs/ и перестраивает индекс

ВЗАИМОДЕЙСТВИЕ С ПОЛЬЗОВАТЕЛЕМ:
  Используется Redis ключ orchestra:gap_requests — туда записываются запросы,
  пользователь отвечает через CLI (orchestra_ctl.py gap-resolve) или Hermes чат.
  Задача, ожидающая souls, паркуется в orchestra:parked_tasks.

ПРАВКИ ДЛЯ СЛЕДУЮЩЕЙ ИТЕРАЦИИ:
  - Изменить промпт генерации новой души → редактируй _generate_soul_llm()
  - Добавить интеграцию с Hermes напрямую (без CLI) → замени _notify_user()
    на прямой вызов Hermes callback
  - Изменить формат текста для upload → редактируй _build_upload_context()
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

import yaml

from modules.models import SoulGapAction, SoulIndexEntry, Task
from modules.redis_bus import get_redis, publish_task, STREAM_SENIOR

log = logging.getLogger("orchestra.soul_gap_resolver")

# ── Структура запроса к пользователю ─────────────────────────────────────

@dataclass
class GapRequest:
    """Запрос к пользователю о недостающей душе."""
    request_id: str
    task_id: str              # задача которая ждёт
    task_content: str         # контент исходной задачи
    direction: str            # нужное направление
    reason: str               # почему именно это направление нужно
    task_for_agent: str       # какую конкретную работу должен делать агент
    similar_directions: list[str]  # похожие души, которые есть
    status: str = "pending"   # pending | resolved | skipped
    action: Optional[str] = None   # create | upload | skip
    resolved_at: Optional[float] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "GapRequest":
        return cls(**d)


class SoulGapResolver:
    """
    Оркестрирует процесс разрешения отсутствующих душ.
    Главный метод: resolve_gaps() — вызывается из SeniorWorker.
    """

    GAP_KEY    = "orchestra:gap_requests"   # Hash: request_id → GapRequest JSON
    PARKED_KEY = "orchestra:parked_tasks"   # Hash: task_id → Task JSON

    def __init__(self, souls_dir: Path, soul_registry):
        self.souls_dir     = souls_dir
        self.soul_registry = soul_registry  # SoulRegistry instance

    # ── Основной поток ────────────────────────────────────────────────────

    def check_and_request(
        self,
        task_id: str,
        task_content: str,
        plan: list[dict],         # [{direction, subtask, count}, ...]
    ) -> tuple[list[dict], list[GapRequest]]:
        """
        Проверяет план задачи на наличие нужных душ.
        Для недостающих — создаёт GapRequest и паркует задачу.

        Возвращает:
          covered_plan   — подзадачи которые можно выполнить прямо сейчас
          gap_requests   — запросы по недостающим направлениям
        """
        covered_plan: list[dict] = []
        gap_requests: list[GapRequest] = []

        for spec in plan:
            direction = spec.get("direction", "")
            subtask   = spec.get("subtask", task_content)

            entry = self.soul_registry.get_entry(direction, "agent")
            if entry:
                covered_plan.append(spec)
            else:
                req = self._create_gap_request(
                    task_id, task_content, direction, subtask
                )
                gap_requests.append(req)
                self._save_gap_request(req)
                log.info("[GapResolver] Нет души для '%s', создан запрос %s",
                         direction, req.request_id)

        return covered_plan, gap_requests

    # ── Парковка задачи (итерация 4) ──────────────────────────────────────

    def park_task(self, task: Task) -> None:
        """
        Сохраняет задачу в парк до момента когда все gap_requests этой
        задачи будут resolved. После resolve задача переотправляется в
        STREAM_SENIOR без потери task_id.
        """
        get_redis().hset(self.PARKED_KEY, task.id, json.dumps(task.to_dict()))
        log.info("[GapResolver] задача %s припаркована", task.id)

    def _unpark_task(self, task_id: str) -> Optional[Task]:
        """Достаёт задачу из парка и удаляет из Redis."""
        r = get_redis()
        raw = r.hget(self.PARKED_KEY, task_id)
        if not raw:
            return None
        r.hdel(self.PARKED_KEY, task_id)
        try:
            return Task.from_dict(json.loads(raw))
        except Exception as exc:
            log.warning("[GapResolver] невалидная парковка %s: %s", task_id, exc)
            return None

    def _all_gaps_resolved_for_task(self, task_id: str) -> bool:
        """True если для task_id нет ни одного pending GapRequest."""
        r = get_redis()
        for raw in r.hvals(self.GAP_KEY):
            try:
                req = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if req.get("task_id") == task_id and req.get("status") == "pending":
                return False
        return True

    def _maybe_republish_parked_tasks(self, resolved_requests: list[GapRequest]) -> list[str]:
        """
        После успешного resolve проходит по task_id-ам затронутых запросов
        и переотправляет в Senior те, у которых больше нет pending-запросов.
        Возвращает список переотправленных task_id.
        """
        republished: list[str] = []
        seen: set[str] = set()
        for req in resolved_requests:
            tid = req.task_id
            if tid in seen:
                continue
            seen.add(tid)

            if not self._all_gaps_resolved_for_task(tid):
                log.info("[GapResolver] %s ещё ждёт другие души", tid)
                continue

            task = self._unpark_task(tid)
            if not task:
                continue

            # Сбрасываем статус на PENDING — Senior снова декомпозирует
            from modules.models import TaskStatus
            task.status      = TaskStatus.PENDING
            task.retry_count = 0
            task.last_error  = None
            publish_task(STREAM_SENIOR, task)
            republished.append(tid)
            log.info("[GapResolver] %s → STREAM_SENIOR (auto-resume)", tid)
        return republished

    def notify_user(self, gap_requests: list[GapRequest]) -> str:
        """
        Формирует текст уведомления для пользователя о недостающих душах.
        Возвращает строку для вывода в CLI / отправки через Hermes.
        """
        lines = [
            "═" * 60,
            "⚠  ОРКЕСТРАТОР ЗАПРАШИВАЕТ НОВЫЕ ДУШИ",
            "═" * 60,
            "",
            f"Для выполнения задачи нужны агенты-направления которых ещё нет.",
            "",
        ]
        for i, req in enumerate(gap_requests, 1):
            lines += [
                f"[{i}] Направление: {req.direction}",
                f"    Зачем нужно: {req.reason}",
                f"    Задача агента: {req.task_for_agent}",
                f"    Похожие (есть): {', '.join(req.similar_directions) or 'нет'}",
                "",
            ]
        lines += [
            "Варианты:",
            "  1) Создать автоматически — AI сформирует душу под каждую задачу",
            "  2) Загрузить вручную — система покажет что именно нужно написать",
            "",
            f"Команда: python orchestra_ctl.py gap-resolve --action create",
            f"     или: python orchestra_ctl.py gap-resolve --action upload",
            "═" * 60,
        ]
        return "\n".join(lines)

    def resolve(
        self,
        action: SoulGapAction,
        uploaded_files: Optional[list[str]] = None,
    ) -> list[str]:
        """
        Разрешает все pending gap_requests.
        action=CREATE: AI генерирует души
        action=UPLOAD: показывает инструкцию, ждёт файлов

        Возвращает список созданных/загруженных directions.
        """
        r = get_redis()
        raw_requests = r.hgetall(self.GAP_KEY)
        pending = [
            GapRequest.from_dict(json.loads(v))
            for v in raw_requests.values()
            if json.loads(v).get("status") == "pending"
        ]
        if not pending:
            log.info("[GapResolver] нет pending запросов")
            return []

        resolved: list[str] = []
        resolved_reqs: list[GapRequest] = []
        for req in pending:
            if action == SoulGapAction.CREATE:
                direction = self._auto_create_soul(req)
            elif action == SoulGapAction.UPLOAD:
                if uploaded_files:
                    direction = self._load_uploaded_soul(req, uploaded_files)
                else:
                    # Показываем инструкцию и ждём
                    print(self._build_upload_context(req))
                    direction = None
            else:
                direction = None  # SKIP

            if direction:
                req.status      = "resolved"
                req.action      = action.value
                req.resolved_at = time.time()
                self._save_gap_request(req)
                resolved.append(direction)
                resolved_reqs.append(req)
                log.info("[GapResolver] resolved '%s' via %s", direction, action.value)

        # Авто-докомплитование припаркованных задач (TODO 1)
        if resolved_reqs:
            republished = self._maybe_republish_parked_tasks(resolved_reqs)
            if republished:
                log.info("[GapResolver] переотправлено в Senior: %d задач",
                         len(republished))

        return resolved

    def get_pending_requests(self) -> list[GapRequest]:
        r = get_redis()
        return [
            GapRequest.from_dict(json.loads(v))
            for v in r.hgetall(self.GAP_KEY).values()
            if json.loads(v).get("status") == "pending"
        ]

    def get_upload_instructions(self) -> str:
        """Возвращает инструкцию для ручной загрузки по всем pending запросам."""
        pending = self.get_pending_requests()
        if not pending:
            return "Нет pending запросов."
        lines = ["═" * 60, "ИНСТРУКЦИЯ ДЛЯ РУЧНОЙ ЗАГРУЗКИ ДУШ", "═" * 60, ""]
        for req in pending:
            lines.append(self._build_upload_context(req))
        return "\n".join(lines)

    # ── Генерация души через AI ───────────────────────────────────────────

    def _auto_create_soul(self, req: GapRequest) -> Optional[str]:
        """
        LLM создаёт новую душу на основе задачи.
        Изучает похожие существующие души для понимания формата,
        но формирует уникальную личность под конкретную задачу.
        """
        similar_souls = self._load_similar_souls(req.similar_directions)
        soul_data     = self._generate_soul_llm(req, similar_souls)
        if not soul_data:
            log.error("[GapResolver] не удалось сгенерировать душу для %s", req.direction)
            return None

        soul_path = self._write_soul_yaml(req.direction, soul_data)
        # Обновляем индекс
        self.soul_registry.index_one(soul_path, req.direction, "agent")
        log.info("[GapResolver] Новая душа создана: %s", soul_path)
        return req.direction

    def _generate_soul_llm(
        self, req: GapRequest, similar_souls: list[dict]
    ) -> Optional[dict]:
        """
        Вызывает LLM для генерации YAML-структуры новой души.
        Даёт LLM примеры похожих душ как образцы ФОРМАТА (не содержания).
        """
        from modules.llm_bridge import call_llm

        examples_text = ""
        for s in similar_souls[:2]:
            examples_text += (
                f"\n--- Пример (направление: {s.get('direction')}) ---\n"
                f"soul.personality: {s.get('personality','')[:200]}\n"
                f"skill.description: {s.get('description','')[:200]}\n"
                f"soul.values: {s.get('values','')}\n"
            )

        system = (
            "Ты создаёшь душу для нового специализированного AI-агента. "
            "Твоя задача — сформировать чёткую, уникальную личность которая "
            "оптимально решает конкретную задачу. "
            "НЕ копируй примеры — используй их только как образцы ФОРМАТА. "
            "Создавай оригинальную личность."
        )
        prompt = (
            f"Направление агента: {req.direction}\n"
            f"Конкретная задача которую он должен решать:\n{req.task_for_agent}\n\n"
            f"Почему это направление нужно:\n{req.reason}\n"
            f"{('Примеры ФОРМАТА (не копировать):\\n' + examples_text) if examples_text else ''}\n\n"
            "Создай YAML-структуру для soul.yaml. Формат:\n"
            "direction: <direction>\n"
            "soul:\n"
            "  personality: <2-3 предложения уникальной личности>\n"
            "  values: [<3-5 ценностей>]\n"
            "  doubt_level: 0.0\n"
            "skill:\n"
            "  version: 1\n"
            "  description: <2 предложения о навыках>\n"
            "  strengths: [<3-5 конкретных сильных сторон>]\n"
            "  learned_patterns: []\n\n"
            "Только YAML. Без markdown-обрамления."
        )
        raw = call_llm(system, [{"role": "user", "content": prompt}], max_tokens=600)
        raw = raw.strip()
        if raw.startswith("```"):
            raw = "\n".join(l for l in raw.split("\n") if not l.startswith("```"))
        try:
            return yaml.safe_load(raw)
        except Exception as exc:
            log.error("Ошибка парсинга сгенерированного YAML: %s\n%s", exc, raw)
            return None

    def _load_similar_souls(self, directions: list[str]) -> list[dict]:
        """Читает похожие soul.yaml для передачи в LLM как примеры формата."""
        result = []
        for direction in directions[:3]:
            soul_file = self.souls_dir / "agents" / direction / "soul.yaml"
            if soul_file.exists():
                try:
                    raw = yaml.safe_load(soul_file.read_text(encoding="utf-8")) or {}
                    soul_data  = raw.get("soul", {})
                    skill_data = raw.get("skill", {})
                    result.append({
                        "direction":   direction,
                        "personality": soul_data.get("personality", ""),
                        "description": skill_data.get("description", ""),
                        "values":      soul_data.get("values", []),
                    })
                except Exception:
                    pass
        return result

    def _write_soul_yaml(self, direction: str, soul_data: dict) -> Path:
        """Записывает сгенерированную душу в SOULs/agents/<direction>/soul.yaml."""
        soul_dir = self.souls_dir / "agents" / direction
        soul_dir.mkdir(parents=True, exist_ok=True)
        soul_path = soul_dir / "soul.yaml"

        header = (
            f"# SOULs/agents/{direction}/soul.yaml\n"
            f"# Создан автоматически orchestra gap-resolver\n"
            f"# Дата: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"# ПРАВКИ: редактируй вручную если AI создал неточность\n\n"
        )
        soul_path.write_text(
            header + yaml.dump(soul_data, allow_unicode=True, default_flow_style=False),
            encoding="utf-8",
        )
        return soul_path

    # ── Загрузка вручную ──────────────────────────────────────────────────

    def _build_upload_context(self, req: GapRequest) -> str:
        """
        Текст для пользователя: что должен делать агент этого направления.
        Пишет настолько детально чтобы пользователь мог создать soul.yaml сам.
        """
        lines = [
            f"┌─ НАПРАВЛЕНИЕ: {req.direction.upper()} ─",
            f"│",
            f"│  Зачем нужен этот агент:",
            f"│  {req.reason}",
            f"│",
            f"│  Конкретная задача которую он должен выполнить:",
            f"│  {req.task_for_agent}",
            f"│",
            f"│  Создай файл: SOULs/agents/{req.direction}/soul.yaml",
            f"│  Формат:",
            f"│",
            f"│    direction: {req.direction}",
            f"│    soul:",
            f"│      personality: |",
            f"│        <личность и подход агента к работе, 2-3 предложения>",
            f"│      values: [<3-5 ценностей через запятую>]",
            f"│      doubt_level: 0.0",
            f"│    skill:",
            f"│      version: 1",
            f"│      description: <что умеет, 1-2 предложения>",
            f"│      strengths: [<3-5 конкретных навыков>]",
            f"│      learned_patterns: []",
            f"│",
        ]
        if req.similar_directions:
            lines += [
                f"│  Похожие существующие души (для вдохновения):",
                f"│  {', '.join(req.similar_directions)}",
                f"│",
            ]
        lines.append("└" + "─" * 50)
        return "\n".join(lines)

    def _load_uploaded_soul(
        self, req: GapRequest, uploaded_files: list[str]
    ) -> Optional[str]:
        """Ищет файл для нужного направления в uploaded_files."""
        for f in uploaded_files:
            p = Path(f)
            if req.direction in p.name or req.direction in str(p):
                # Копируем в правильное место
                target = self.souls_dir / "agents" / req.direction / "soul.yaml"
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(p.read_text(encoding="utf-8"), encoding="utf-8")
                self.soul_registry.index_one(target, req.direction, "agent")
                log.info("[GapResolver] Загружена душа из %s → %s", f, target)
                return req.direction
        log.warning("[GapResolver] файл для '%s' не найден среди загруженных", req.direction)
        return None

    # ── Вспомогательные ───────────────────────────────────────────────────

    def _create_gap_request(
        self,
        task_id: str,
        task_content: str,
        direction: str,
        subtask: str,
    ) -> GapRequest:
        """Формирует GapRequest с описанием задачи для агента."""
        # Находим похожие направления по имени
        all_entries = self.soul_registry.get_all("agent")
        similar = self._find_similar_directions(direction, all_entries)

        # Кратко описываем зачем нужен агент этого направления
        reason = self._describe_reason(direction, subtask)

        import uuid
        return GapRequest(
            request_id=f"gap_{uuid.uuid4().hex[:8]}",
            task_id=task_id,
            task_content=task_content,
            direction=direction,
            reason=reason,
            task_for_agent=subtask,
            similar_directions=similar,
        )

    def _describe_reason(self, direction: str, subtask: str) -> str:
        """Коротко объясняет зачем нужен агент данного направления."""
        return (
            f"Для задачи требуется специалист по '{direction}'. "
            f"Подзадача: {subtask[:200]}"
        )

    def _find_similar_directions(
        self, direction: str, all_entries: list[SoulIndexEntry]
    ) -> list[str]:
        """Находит похожие направления по сходству имени (простая эвристика)."""
        direction_words = set(direction.lower().replace("_", " ").split())
        similar = []
        for entry in all_entries:
            entry_words = set(entry.direction.lower().replace("_", " ").split())
            if direction_words & entry_words:
                similar.append(entry.direction)
        return similar[:3]

    def _save_gap_request(self, req: GapRequest) -> None:
        get_redis().hset(self.GAP_KEY, req.request_id, json.dumps(req.to_dict()))
