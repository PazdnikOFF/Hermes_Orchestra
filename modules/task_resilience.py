"""
modules/task_resilience.py — устойчивость задач к сбоям агентов.

Обеспечивает:
  1. Retry с exponential backoff при падении агента
  2. Dead Letter Queue (DLQ) после исчерпания retry
  3. Делегирование задачи новому агенту при падении текущего
  4. Heartbeat мониторинг — обнаружение зависших агентов
  5. Защита от бесконечного retry (retry_max в Task)

АРХИТЕКТУРА:
  - Каждый AgentWorker пишет heartbeat в Redis каждые HEARTBEAT_INTERVAL сек
  - ResilienceWatcher (запускается внутри WatchdogWorker) проверяет heartbeats
  - Если heartbeat устарел — задача агента помечается FAILED и отправляется на retry
  - После retry_max попыток → DLQ, уведомление пользователя

КЛЮЧИ REDIS:
  heartbeat:{agent_id}            String  — timestamp последнего heartbeat
  dlq:tasks                       Stream  — задачи в dead letter queue
  dlq:log                         List    — история DLQ (для диагностики)
  task:assigned:{task_id}         Hash    — кому назначена задача и когда

ПРАВКИ ДЛЯ СЛЕДУЮЩЕЙ ИТЕРАЦИИ:
  - Изменить backoff → редактируй _backoff_seconds()
  - Изменить TTL heartbeat → редактируй HEARTBEAT_TTL
  - Добавить алерт при DLQ → добавь вызов в _send_to_dlq()
  - Изменить логику выбора нового агента → редактируй _pick_retry_agent()
"""

from __future__ import annotations

import json
import logging
import math
import time
from typing import Optional

from modules.models import Task, TaskStatus
from modules.redis_bus import (
    AgentRegistry, ack_task, get_redis, publish_task, read_tasks, stream_agent
)

log = logging.getLogger("orchestra.resilience")

HEARTBEAT_INTERVAL = 5     # секунд между heartbeat-записями
HEARTBEAT_TTL      = 30    # секунд — после этого агент считается мёртвым
DLQ_STREAM         = "dlq:tasks"
DLQ_LOG_KEY        = "dlq:log"
ASSIGN_PREFIX      = "task:assigned"
MAX_RETRY_DELAY    = 60    # максимальная задержка между retry (сек)


# ── Heartbeat ─────────────────────────────────────────────────────────────

def heartbeat_write(agent_id: str) -> None:
    """Агент пишет heartbeat. Вызывается из AgentWorker в основном цикле."""
    get_redis().setex(f"heartbeat:{agent_id}", HEARTBEAT_TTL, str(time.time()))


def heartbeat_alive(agent_id: str) -> bool:
    """True если heartbeat агента свежий (агент живой)."""
    val = get_redis().get(f"heartbeat:{agent_id}")
    if not val:
        return False
    return (time.time() - float(val)) < HEARTBEAT_TTL


# ── Task assignment tracking ───────────────────────────────────────────────

def assign_task(task: Task, agent_id: str) -> None:
    """Регистрируем назначение задачи агенту (для мониторинга)."""
    r = get_redis()
    r.hset(f"{ASSIGN_PREFIX}:{task.id}", mapping={
        "agent_id":    agent_id,
        "assigned_at": str(time.time()),
        "retry_count": str(task.retry_count),
        "task":        json.dumps(task.to_dict()),
    })
    # TTL чтобы запись не висела вечно
    r.expire(f"{ASSIGN_PREFIX}:{task.id}", 3600)


def clear_assignment(task_id: str) -> None:
    get_redis().delete(f"{ASSIGN_PREFIX}:{task_id}")


def get_assignment(task_id: str) -> Optional[dict]:
    raw = get_redis().hgetall(f"{ASSIGN_PREFIX}:{task_id}")
    return raw if raw else None


# ── Retry ─────────────────────────────────────────────────────────────────

def schedule_retry(
    task: Task,
    error: str,
    original_stream: str,
    exclude_agent_id: Optional[str] = None,
) -> Optional[Task]:
    """
    Планирует повторное выполнение задачи.

    Если retry_count < retry_max:
      - Увеличивает retry_count
      - Ждёт backoff секунд
      - Назначает другому агенту (если возможно)
      - Публикует обратно в поток
      - Возвращает обновлённый Task

    Если retry_count >= retry_max:
      - Отправляет в DLQ
      - Возвращает None
    """
    task.retry_count += 1
    task.last_error   = error[:500]
    task.status       = TaskStatus.RETRYING

    if not task.can_retry():
        _send_to_dlq(task, original_stream)
        return None

    delay = _backoff_seconds(task.retry_count)
    log.warning(
        "[Resilience] задача %s → retry %d/%d через %.1f сек (причина: %s)",
        task.id, task.retry_count, task.retry_max, delay, error[:100]
    )

    # Ждём backoff (в реальной системе — через delayed queue или Redis EXPIRE)
    # Здесь используем простую задержку перед публикацией
    time.sleep(min(delay, MAX_RETRY_DELAY))

    # Пробуем выбрать другого агента
    new_agent = _pick_retry_agent(task.direction, exclude_agent_id)
    target_stream = stream_agent(new_agent.id) if new_agent else original_stream

    task.assigned_agent_id = new_agent.id if new_agent else None
    publish_task(target_stream, task)

    log.info(
        "[Resilience] задача %s переназначена → %s (stream=%s)",
        task.id, new_agent.id if new_agent else "original", target_stream
    )
    return task


def _backoff_seconds(retry_count: int) -> float:
    """Exponential backoff: 2^retry * base, capped at MAX_RETRY_DELAY."""
    base = 2.0
    return min(base ** retry_count, MAX_RETRY_DELAY)


def _pick_retry_agent(direction: str, exclude_id: Optional[str] = None):
    """
    Выбирает агента для повторного выполнения.
    Исключает упавшего агента. Предпочитает агентов с живым heartbeat.
    """
    from modules.models import AgentRole
    agents = [
        a for a in AgentRegistry.by_direction(direction)
        if a.role in (AgentRole.AGENT, AgentRole.AGENT_ORCHESTRATOR)
        and a.id != exclude_id
    ]
    if not agents:
        return None

    # Предпочитаем агентов с живым heartbeat
    alive = [a for a in agents if heartbeat_alive(a.id)]
    return alive[0] if alive else agents[0]


# ── Dead Letter Queue ──────────────────────────────────────────────────────

def _send_to_dlq(task: Task, original_stream: str) -> None:
    """Отправляет задачу в DLQ и логирует."""
    task.status = TaskStatus.DLQ
    r = get_redis()

    # Добавляем в DLQ stream
    r.xadd(DLQ_STREAM, {
        "task":            json.dumps(task.to_dict()),
        "original_stream": original_stream,
        "dlq_at":          str(time.time()),
    })

    # Лог для диагностики
    entry = {
        "task_id":         task.id,
        "direction":       task.direction,
        "last_error":      task.last_error,
        "retry_count":     task.retry_count,
        "dlq_at":          time.time(),
        "original_stream": original_stream,
    }
    r.rpush(DLQ_LOG_KEY, json.dumps(entry))
    r.ltrim(DLQ_LOG_KEY, -200, -1)  # храним последние 200 записей

    log.error(
        "[Resilience] DLQ: задача %s (direction=%s) исчерпала %d retry. "
        "Запусти: python orchestra_ctl.py dlq-list",
        task.id, task.direction, task.retry_count
    )

    # Записываем в results для видимости в status
    from modules.redis_bus import save_result
    save_result(task.id, "status", "dlq")
    save_result(task.id, "last_error", task.last_error or "")
    save_result(task.id, "retry_count", str(task.retry_count))


def get_dlq_tasks(count: int = 20) -> list[dict]:
    """Читает задачи из DLQ для просмотра/переотправки."""
    r = get_redis()
    try:
        entries = r.xrange(DLQ_STREAM, count=count)
        result = []
        for entry_id, fields in entries:
            try:
                t = json.loads(fields.get("task", "{}"))
                t["dlq_at"] = fields.get("dlq_at", "")
                result.append(t)
            except Exception:
                pass
        return result
    except Exception:
        return []


def retry_dlq_task(task_id: str) -> bool:
    """
    Принудительно переотправляет задачу из DLQ.
    Сбрасывает retry_count, выбирает нового агента.
    """
    r = get_redis()
    entries = r.xrange(DLQ_STREAM, count=100)
    for entry_id, fields in entries:
        try:
            t = Task.from_dict(json.loads(fields.get("task", "{}")))
            if t.id != task_id:
                continue

            t.status      = TaskStatus.PENDING
            t.retry_count = 0
            t.last_error  = None

            agent = _pick_retry_agent(t.direction)
            if agent:
                publish_task(stream_agent(agent.id), t)
                log.info("[Resilience] DLQ task %s переотправлена → %s", task_id, agent.id)
                return True
            else:
                log.warning("[Resilience] нет агента для %s, задача осталась в DLQ", task_id)
                return False
        except Exception as exc:
            log.error("[Resilience] ошибка retry DLQ %s: %s", task_id, exc)
    return False


# ── Watcher (запускается внутри WatchdogWorker) ────────────────────────────

class ResilienceWatcher:
    """
    Проверяет heartbeats агентов.
    При обнаружении мёртвого агента с активной задачей — переназначает задачу.
    """

    def check_all(self) -> int:
        """Проверяет всех агентов. Возвращает количество переназначенных задач."""
        from modules.models import AgentRole
        reassigned = 0
        for agent in AgentRegistry.all_agents():
            if agent.role not in (AgentRole.AGENT, AgentRole.AGENT_ORCHESTRATOR):
                continue
            if not heartbeat_alive(agent.id):
                # Агент мёртв — ищем его активные задачи
                stuck = self._find_stuck_tasks(agent.id)
                for task, original_stream in stuck:
                    result = schedule_retry(task, "agent heartbeat timeout", original_stream,
                                            exclude_agent_id=agent.id)
                    if result:
                        reassigned += 1
        return reassigned

    def _find_stuck_tasks(self, agent_id: str) -> list[tuple[Task, str]]:
        """Ищет задачи назначенные агенту которые ещё не завершены."""
        r = get_redis()
        original_stream = stream_agent(agent_id)

        # SCAN вместо KEYS — не блокирует Redis на больших множествах.
        stuck: list[tuple[Task, str]] = []
        for key in r.scan_iter(match=f"{ASSIGN_PREFIX}:*", count=200):
            raw = r.hgetall(key)
            if not raw or raw.get("agent_id") != agent_id:
                continue
            try:
                task = Task.from_dict(json.loads(raw.get("task", "{}")))
                if task.status in (TaskStatus.RUNNING, TaskStatus.PENDING):
                    stuck.append((task, original_stream))
            except Exception:
                pass
        return stuck

    # ── Recovery после рестарта системы (TODO 5) ──────────────────────────

    def recover_on_startup(self) -> int:
        """
        Сканирует ASSIGN_PREFIX:* и переотправляет задачи, которые провисели
        в назначенных дольше чем 2× HEARTBEAT_TTL без живого агента.

        Вызывается из WatchdogWorker.run() ОДИН РАЗ после grace-периода.
        Это закрывает случай когда система была остановлена с задачами в
        статусе RUNNING — pending-сообщения Redis Streams к моменту запуска
        не разлапываются стандартным XREADGROUP с курсором '>'.

        Возвращает количество переотправленных задач.
        """
        r = get_redis()
        cutoff = time.time() - HEARTBEAT_TTL * 2
        recovered = 0

        for key in r.scan_iter(match=f"{ASSIGN_PREFIX}:*", count=200):
            raw = r.hgetall(key)
            if not raw:
                continue
            try:
                assigned_at = float(raw.get("assigned_at", "0"))
            except ValueError:
                assigned_at = 0.0
            if assigned_at >= cutoff:
                continue  # свежее назначение — Watcher.check_all разберётся

            agent_id = raw.get("agent_id", "")
            if agent_id and heartbeat_alive(agent_id):
                continue  # агент жив — пусть доделает

            try:
                task = Task.from_dict(json.loads(raw.get("task", "{}")))
            except Exception:
                continue
            if task.status not in (TaskStatus.RUNNING, TaskStatus.PENDING,
                                   TaskStatus.RETRYING):
                continue

            result = schedule_retry(
                task,
                "startup recovery: orphaned assignment",
                stream_agent(agent_id) if agent_id else stream_agent(task.direction),
                exclude_agent_id=agent_id or None,
            )
            if result:
                recovered += 1
        return recovered
