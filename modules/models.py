"""
modules/models.py — все модели данных оркестра.

Только dataclasses и enum'ы. Никакой бизнес-логики.

ПРАВКИ ДЛЯ СЛЕДУЮЩЕЙ ИТЕРАЦИИ:
  - Новое поле в AgentState → добавь сюда + в to_redis() + from_redis()
  - Новый AgentRole → добавь в enum + обнови workers которые проверяют role
  - Новый TaskStatus → добавь в enum + обнови логику в judge_worker.py
  - Новое поле в Task → не забудь to_dict() и from_dict()
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Optional


# ── Enums ─────────────────────────────────────────────────────────────────

class AgentRole(str, Enum):
    SENIOR             = "SENIOR"           # бессмертный, singleton
    JUDGE              = "JUDGE"            # singleton
    MIDDLE             = "MIDDLE"           # динамический под задачу
    JUNIOR             = "JUNIOR"           # динамический под задачу
    AGENT              = "AGENT"            # исполнитель
    AGENT_ORCHESTRATOR = "AGENT_ORCHESTRATOR"  # исполнитель, повышен автоскейлером


class TaskStatus(str, Enum):
    PENDING    = "pending"
    RUNNING    = "running"
    DONE       = "done"
    FAILED     = "failed"
    VALIDATING = "validating"
    RETRYING   = "retrying"    # ← новый: задача повторяется после сбоя
    DLQ        = "dlq"         # ← новый: исчерпаны retry, в dead-letter queue


class SoulGapAction(str, Enum):
    """Ответ пользователя когда нет подходящей души."""
    CREATE = "create"   # AI создаёт новую душу
    UPLOAD = "upload"   # пользователь загружает файл вручную
    SKIP   = "skip"     # использовать ближайший аналог


# ── Модели ────────────────────────────────────────────────────────────────

@dataclass
class Soul:
    personality: str
    values: list[str]
    doubt_level: float = 0.0

    def to_prompt(self) -> str:
        parts = [self.personality.strip()]
        if self.values:
            parts.append("Твои ценности: " + ", ".join(self.values) + ".")
        if self.doubt_level > 0:
            parts.append(
                f"ВАЖНО: Ты подходишь к каждому результату со скептицизмом "
                f"(уровень сомнения {self.doubt_level:.0%}). "
                f"Твоя задача — найти что неправильно, а не подтвердить."
            )
        return " ".join(parts)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Soul":
        return cls(**d)


@dataclass
class Skill:
    version: int
    description: str
    strengths: list[str] = field(default_factory=list)
    learned_patterns: list[str] = field(default_factory=list)
    last_updated: float = field(default_factory=time.time)

    def to_prompt(self) -> str:
        lines = [f"Твой скилл (v{self.version}): {self.description.strip()}"]
        if self.strengths:
            lines.append("Сильные стороны: " + "; ".join(self.strengths))
        if self.learned_patterns:
            lines.append("Усвоенные паттерны: " + "; ".join(self.learned_patterns[-5:]))
        return " ".join(lines)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Skill":
        return cls(**d)


@dataclass
class SoulIndexEntry:
    """
    Запись в динамическом каталоге душ (soul_registry.py).
    Хранится в Redis и перестраивается при изменении SOULs/.
    """
    direction: str          # "math" | "coder" | ...
    role_type: str          # "agent" | "orchestrator"
    path: str               # абсолютный путь к soul.yaml
    capabilities: list[str] # ключевые способности, извлечённые LLM
    tags: list[str]         # теги для быстрого поиска
    soul_preview: str       # первые 120 символов personality
    skill_preview: str      # первые 120 символов description
    last_indexed: float = field(default_factory=time.time)
    # Опциональное embedding-описание (итерация 4). Пусто если не считалось.
    embedding: list[float] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "SoulIndexEntry":
        known = {f for f in cls.__dataclass_fields__}
        d = {k: v for k, v in d.items() if k in known}
        return cls(**d)


@dataclass
class AgentState:
    id: str
    role: AgentRole
    direction: str
    soul: Soul
    skill: Skill
    status: str = "idle"

    # Иерархия
    parent_agent_id: Optional[str] = None
    clone_ids: list[str] = field(default_factory=list)

    # Метрики
    avg_latency_ms: float = 0.0
    tasks_completed: int = 0
    last_task_at: float = 0.0
    last_active_at: float = field(default_factory=time.time)

    # Мета
    immortal: bool = False
    singleton: bool = False

    # ── Redis сериализация ─────────────────────────────────────────────

    def to_redis(self) -> dict[str, str]:
        return {
            "id":               self.id,
            "role":             self.role.value,
            "direction":        self.direction,
            "soul":             json.dumps(self.soul.to_dict()),
            "skill":            json.dumps(self.skill.to_dict()),
            "status":           self.status,
            "parent_agent_id":  self.parent_agent_id or "",
            "clone_ids":        json.dumps(self.clone_ids),
            "avg_latency_ms":   str(self.avg_latency_ms),
            "tasks_completed":  str(self.tasks_completed),
            "last_task_at":     str(self.last_task_at),
            "last_active_at":   str(self.last_active_at),
            "immortal":         "1" if self.immortal else "0",
            "singleton":        "1" if self.singleton else "0",
        }

    @classmethod
    def from_redis(cls, d: dict) -> "AgentState":
        return cls(
            id=d["id"],
            role=AgentRole(d["role"]),
            direction=d["direction"],
            soul=Soul.from_dict(json.loads(d["soul"])),
            skill=Skill.from_dict(json.loads(d["skill"])),
            status=d.get("status", "idle"),
            parent_agent_id=d.get("parent_agent_id") or None,
            clone_ids=json.loads(d.get("clone_ids", "[]")),
            avg_latency_ms=float(d.get("avg_latency_ms", 0)),
            tasks_completed=int(d.get("tasks_completed", 0)),
            last_task_at=float(d.get("last_task_at", 0)),
            last_active_at=float(d.get("last_active_at", 0)),
            immortal=d.get("immortal", "0") == "1",
            singleton=d.get("singleton", "0") == "1",
        )


@dataclass
class Task:
    id: str
    content: str
    direction: str
    source_agent_id: str
    status: TaskStatus = TaskStatus.PENDING
    result: Optional[str] = None
    verdict: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    finished_at: Optional[float] = None

    # ── Resilience поля ──────────────────────────────────────────────
    retry_count: int = 0          # сколько раз уже пытались
    retry_max: int = 3            # максимум попыток (потом → DLQ)
    last_error: Optional[str] = None   # последняя ошибка (для диагностики)
    assigned_agent_id: Optional[str] = None  # кому назначена сейчас

    # ── Иерархия (итерация 4) ────────────────────────────────────────
    # parent_task_id — id корневой задачи пользователя; используется
    # JudgeWorker для проверки готовности всех сабтасков и автосборки.
    parent_task_id: Optional[str] = None

    # ── Judge rework loop (итерация 4) ───────────────────────────────
    # Сколько раз Judge уже отправлял эту задачу на доработку.
    # При превышении CFG.judge_max_iterations Judge сам выбирает лучший
    # результат из history и финализирует задачу.
    judge_iteration: int = 0
    # Текст критики от Judge, который AgentWorker подмешивает в system prompt
    # при повторном выполнении. None для первой попытки.
    rework_context: Optional[str] = None

    def latency_ms(self) -> Optional[float]:
        if self.started_at and self.finished_at:
            return (self.finished_at - self.started_at) * 1000
        return None

    def can_retry(self) -> bool:
        return self.retry_count < self.retry_max

    def to_dict(self) -> dict:
        d = asdict(self)
        d["status"] = self.status.value
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "Task":
        # Толерантно игнорируем неизвестные поля для совместимости со старыми
        # сериализациями в Redis после расширения схемы.
        known = {f for f in cls.__dataclass_fields__}
        d = {k: v for k, v in d.items() if k in known}
        d["status"] = TaskStatus(d.get("status", "pending"))
        return cls(**d)


# ── Вспомогательные функции ───────────────────────────────────────────────

def make_agent_id(direction: str, suffix: str = "") -> str:
    slug = suffix or uuid.uuid4().hex[:8]
    return f"{direction}_{slug}"
