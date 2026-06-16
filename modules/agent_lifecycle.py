"""
modules/agent_lifecycle.py — рождение и смерть direction-агентов.

ПРИНЦИП:
  Middle/Junior/Agent для одного direction живут пока хотя бы одна корневая
  задача их использует. Senior на старте каждой задачи "арендует" direction
  (refcount++), ResultAssembler после финала "отпускает" (refcount--).
  Когда refcount достигает 0 — teardown:
    1. Убить subprocess'ы (Middle/Junior/Agent) по сохранённому PID
    2. Удалить worker:running:* и worker:pid:* локи
    3. Удалить AgentState'ы из реестра
    4. Heartbeat'ы оставляем — они сами протухнут по TTL

Эволюция душ/скиллов УЖЕ происходит per-subtask внутри JudgeWorker
(SoulEvolver + maybe_evolve_skill), поэтому teardown не делает доп. evolve.

КЛЮЧИ REDIS:
  orchestra:dir_refcount        Hash  direction → счётчик активных задач
  dir_acquired:{task_id}        Set   направления, занятые этой корневой задачей
  worker:pid:{kind}:{direction} Str   PID запущенного subprocess'а (для kill)

ПРАВКИ:
  - Изменить какие роли тирдаунить — расширь _ROLES_TO_TEARDOWN
  - Изменить таймаут на graceful SIGTERM перед SIGKILL — _GRACEFUL_SHUTDOWN_SEC
"""

from __future__ import annotations

import logging
import os
import signal as sig
import time
from typing import Optional

from modules.models import AgentRole
from modules.redis_bus import AgentRegistry, get_redis

log = logging.getLogger("orchestra.lifecycle")

REFCOUNT_KEY        = "orchestra:dir_refcount"
ACQUIRED_KEY_PREFIX = "dir_acquired"
PID_KEY_PREFIX      = "worker:pid"
LOCK_KEY_PREFIX     = "worker:running"
ACQUIRE_TTL         = 86400      # 24 ч — safety net на случай если задача застряла
_GRACEFUL_SHUTDOWN_SEC = 3

_ROLES_TO_TEARDOWN = (AgentRole.MIDDLE, AgentRole.JUNIOR,
                      AgentRole.AGENT, AgentRole.AGENT_ORCHESTRATOR)


# ── PID tracking ──────────────────────────────────────────────────────────

def register_worker_pid(kind: str, direction: str, pid: int) -> None:
    """Вызывается из _spawn_worker_subprocess сразу после Popen."""
    if not direction:
        return
    get_redis().setex(f"{PID_KEY_PREFIX}:{kind}:{direction}", 86400, str(pid))


def _get_worker_pid(kind: str, direction: str) -> Optional[int]:
    raw = get_redis().get(f"{PID_KEY_PREFIX}:{kind}:{direction}")
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


# ── Refcounting ───────────────────────────────────────────────────────────

def acquire_direction(parent_task_id: str, direction: str) -> int:
    """
    Senior вызывает для каждого direction корневой задачи.
    Идемпотентно по (parent_task_id, direction) — повторный acquire не
    увеличивает счётчик.
    Возвращает текущий refcount.
    """
    if not parent_task_id or not direction:
        return 0
    r = get_redis()
    acquired_key = f"{ACQUIRED_KEY_PREFIX}:{parent_task_id}"
    is_new = r.sadd(acquired_key, direction)
    r.expire(acquired_key, ACQUIRE_TTL)
    if is_new:
        n = r.hincrby(REFCOUNT_KEY, direction, 1)
        log.info("[lifecycle] %s acquired '%s' (refcount=%d)",
                 parent_task_id, direction, n)
        return n
    return int(r.hget(REFCOUNT_KEY, direction) or 0)


def release_directions_for(parent_task_id: str, factory=None) -> list[str]:
    """
    ResultAssembler вызывает по завершении корневой задачи.
    Освобождает все занятые ею direction'ы; если refcount упал до 0 —
    triggers teardown subprocess'ов и AgentState'ов.

    Возвращает список реально снесённых direction'ов (для логирования).
    """
    if not parent_task_id:
        return []
    r = get_redis()
    acquired_key = f"{ACQUIRED_KEY_PREFIX}:{parent_task_id}"
    directions = r.smembers(acquired_key)
    if not directions:
        return []

    torn_down: list[str] = []
    for d in directions:
        n = r.hincrby(REFCOUNT_KEY, d, -1)
        if n <= 0:
            r.hdel(REFCOUNT_KEY, d)
            if teardown_direction(d, factory):
                torn_down.append(d)
        else:
            log.info("[lifecycle] %s released '%s' (refcount=%d, остался)",
                     parent_task_id, d, n)
    r.delete(acquired_key)
    return torn_down


# ── Teardown ──────────────────────────────────────────────────────────────

def teardown_direction(direction: str, factory=None) -> bool:
    """
    Убивает subprocess'ы Middle/Junior/Agent для direction, удаляет
    AgentState'ы и worker-локи. Безопасно вызывать когда refcount=0.

    Возвращает True если что-то снесено.
    """
    r = get_redis()
    something = False

    # 1. Kill subprocess'ы по сохранённым PID
    for kind in ("agent", "junior", "middle"):
        pid = _get_worker_pid(kind, direction)
        if pid:
            if _kill_pid(pid):
                log.info("[lifecycle] killed %s/%s pid=%d", kind, direction, pid)
                something = True
            r.delete(f"{PID_KEY_PREFIX}:{kind}:{direction}")
        # Снимаем lock — следующий task сможет переподнять
        r.delete(f"{LOCK_KEY_PREFIX}:{kind}:{direction}")

    # 2. Удалить AgentState'ы для direction (Middle/Junior/Agent)
    for agent in AgentRegistry.by_direction(direction):
        if agent.role in _ROLES_TO_TEARDOWN and not agent.immortal:
            AgentRegistry.delete(agent.id)
            log.info("[lifecycle] AgentState %s удалён", agent.id)
            something = True

    return something


def _kill_pid(pid: int) -> bool:
    """SIGTERM → ждём grace → SIGKILL если живой."""
    try:
        os.kill(pid, sig.SIGTERM)
    except ProcessLookupError:
        return False
    except Exception as exc:
        log.warning("[lifecycle] SIGTERM pid=%d failed: %s", pid, exc)
        return False

    # Graceful wait
    for _ in range(_GRACEFUL_SHUTDOWN_SEC * 10):
        try:
            os.kill(pid, 0)  # ping
            time.sleep(0.1)
        except ProcessLookupError:
            return True

    # Не сдох — SIGKILL
    try:
        os.kill(pid, sig.SIGKILL)
        log.warning("[lifecycle] pid=%d не ответил на SIGTERM, SIGKILL", pid)
    except Exception:
        pass
    return True
