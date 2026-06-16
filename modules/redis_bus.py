"""
modules/redis_bus.py — все операции с Redis.

Ни один другой модуль не импортирует redis напрямую.
Всё общение с Redis проходит через этот модуль.

СТРУКТУРА КЛЮЧЕЙ:
  agent:{id}:state          Hash    — состояние агента
  agent:{id}:skill_history  List    — лог эволюции скилла
  orchestra:agents          Hash    — индекс agent_id → direction
  orchestra:config          Hash    — runtime конфиг
  tasks:senior              Stream  — задачи для Senior
  tasks:middle:{direction}  Stream  — подзадачи по направлению
  tasks:agent:{id}          Stream  — микрозадачи для конкретного агента
  metrics:latency:{dir}     ZSet    — score=avg_ms, member=agent_id
  metrics:throughput        Hash    — agent_id → tasks_completed
  judge:output_check        Stream  — результаты на проверку Judge
  judge:verdicts            Hash    — task_id → verdict JSON
  results:{task_id}         Hash    — итоговый результат

ПРАВКИ ДЛЯ СЛЕДУЮЩЕЙ ИТЕРАЦИИ:
  - Добавить новый тип потока → добавь константу STREAMS и метод publish_*/read_*
  - Изменить TTL ключей → добавь r.expire() в соответствующий save-метод
  - Добавить новое поле в AgentState → обнови AgentRegistry.save/load в models.py
"""

from __future__ import annotations

import json
import logging
import time
from typing import Optional

import redis as redis_lib

from modules.models import AgentRole, AgentState, Skill, Soul, Task, TaskStatus

log = logging.getLogger("orchestra.redis_bus")

# ── Имена потоков ─────────────────────────────────────────────────────────

STREAM_SENIOR     = "tasks:senior"
STREAM_JUDGE_IN   = "judge:output_check"

def stream_middle(direction: str) -> str:
    return f"tasks:middle:{direction}"

def stream_agent(agent_id: str) -> str:
    return f"tasks:agent:{agent_id}"


# ── Redis соединение ──────────────────────────────────────────────────────

_client: Optional[redis_lib.Redis] = None

def get_redis(url: str = "") -> redis_lib.Redis:
    """Lazy singleton. url используется только при первом вызове."""
    global _client
    if _client is None:
        from modules.config import CFG
        _client = redis_lib.from_url(url or CFG.redis_url, decode_responses=True)
    return _client

def ping() -> bool:
    try:
        get_redis().ping()
        return True
    except Exception:
        return False


# ── AgentRegistry ─────────────────────────────────────────────────────────

class AgentRegistry:
    """CRUD для AgentState в Redis."""

    @staticmethod
    def _state_key(agent_id: str) -> str:
        return f"agent:{agent_id}:state"

    @staticmethod
    def _skill_history_key(agent_id: str) -> str:
        return f"agent:{agent_id}:skill_history"

    @staticmethod
    def _index_key() -> str:
        return "orchestra:agents"

    @classmethod
    def save(cls, agent: AgentState) -> None:
        r = get_redis()
        r.hset(cls._state_key(agent.id), mapping=agent.to_redis())
        r.hset(cls._index_key(), agent.id, agent.direction)
        log.debug("Saved agent %s (role=%s)", agent.id, agent.role.value)

    @classmethod
    def load(cls, agent_id: str) -> Optional[AgentState]:
        raw = get_redis().hgetall(cls._state_key(agent_id))
        if not raw:
            return None
        try:
            return AgentState.from_redis(raw)
        except Exception as exc:
            log.error("Failed to deserialise agent %s: %s", agent_id, exc)
            return None

    @classmethod
    def delete(cls, agent_id: str) -> None:
        """Удаляет агента. Immortal агентов не удаляет (проверяет флаг)."""
        agent = cls.load(agent_id)
        if agent and agent.immortal:
            log.warning("Attempt to delete immortal agent %s — ignored", agent_id)
            return
        r = get_redis()
        r.delete(cls._state_key(agent_id))
        r.hdel(cls._index_key(), agent_id)
        log.info("Deleted agent %s", agent_id)

    @classmethod
    def all_ids(cls) -> list[str]:
        return list(get_redis().hkeys(cls._index_key()))

    @classmethod
    def all_agents(cls) -> list[AgentState]:
        # Батчим HGETALL через pipeline: один round-trip вместо N (было N+1).
        # Горячий путь: Junior._dispatch, _pick_retry_agent, Watchdog каждые 10с.
        ids = cls.all_ids()
        if not ids:
            return []
        r = get_redis()
        pipe = r.pipeline()
        for aid in ids:
            pipe.hgetall(cls._state_key(aid))
        raws = pipe.execute()

        agents = []
        for aid, raw in zip(ids, raws):
            if not raw:
                continue  # агент удалён между HKEYS и HGETALL — пропускаем
            try:
                agents.append(AgentState.from_redis(raw))
            except Exception as exc:
                log.error("Failed to deserialise agent %s: %s", aid, exc)
        return agents

    @classmethod
    def by_direction(cls, direction: str) -> list[AgentState]:
        return [a for a in cls.all_agents() if a.direction == direction]

    @classmethod
    def by_role(cls, role: AgentRole) -> list[AgentState]:
        return [a for a in cls.all_agents() if a.role == role]

    @classmethod
    def find_senior(cls) -> Optional[AgentState]:
        seniors = cls.by_role(AgentRole.SENIOR)
        return seniors[0] if seniors else None

    # ── Метрики ───────────────────────────────────────────────────────────

    @classmethod
    def update_metrics(cls, agent_id: str, latency_ms: float) -> None:
        r = get_redis()
        agent = cls.load(agent_id)
        if not agent:
            return
        n = agent.tasks_completed
        agent.avg_latency_ms = (agent.avg_latency_ms * n + latency_ms) / (n + 1)
        agent.tasks_completed = n + 1
        agent.last_task_at    = time.time()
        agent.last_active_at  = time.time()
        cls.save(agent)
        r.zadd(f"metrics:latency:{agent.direction}", {agent_id: agent.avg_latency_ms})
        r.hincrbyfloat("metrics:throughput", agent_id, 1)

    # ── История скилла ────────────────────────────────────────────────────

    @classmethod
    def append_skill_history(cls, agent_id: str, entry: dict) -> None:
        get_redis().rpush(cls._skill_history_key(agent_id), json.dumps(entry))

    @classmethod
    def get_skill_history(cls, agent_id: str, last_n: int = 20) -> list[dict]:
        raw = get_redis().lrange(cls._skill_history_key(agent_id), -last_n, -1)
        return [json.loads(x) for x in raw]

    @classmethod
    def reset_skill_history(cls, agent_id: str) -> None:
        get_redis().delete(cls._skill_history_key(agent_id))


# ── Stream helpers ────────────────────────────────────────────────────────

def publish_task(stream: str, task: Task) -> str:
    """XADD задачу в поток. Возвращает Redis entry ID."""
    entry_id = get_redis().xadd(stream, {"task": json.dumps(task.to_dict())})
    log.debug("Published task %s → %s", task.id, stream)
    return entry_id


def read_tasks(
    stream: str,
    group: str,
    consumer: str,
    count: int = 1,
    block_ms: int = 5000,
) -> list[tuple[str, Task]]:
    """
    XREADGROUP из потока. Создаёт группу если нет.
    Возвращает список (entry_id, Task).
    """
    r = get_redis()
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis_lib.ResponseError:
        pass  # группа уже существует

    raw = r.xreadgroup(group, consumer, {stream: ">"}, count=count, block=block_ms)
    if not raw:
        return []

    results = []
    for _stream, messages in raw:
        for entry_id, fields in messages:
            try:
                task = Task.from_dict(json.loads(fields["task"]))
                results.append((entry_id, task))
            except Exception as exc:
                log.warning("Malformed task in %s: %s", stream, exc)
                r.xack(stream, group, entry_id)
    return results


def ack_task(stream: str, group: str, entry_id: str) -> None:
    get_redis().xack(stream, group, entry_id)


# ── Results store ─────────────────────────────────────────────────────────

def save_result(task_id: str, key: str, value: str) -> None:
    get_redis().hset(f"results:{task_id}", key, value)

def load_result(task_id: str) -> dict:
    return get_redis().hgetall(f"results:{task_id}")

def save_verdict(task_id: str, verdict: dict) -> None:
    get_redis().hset("judge:verdicts", task_id, json.dumps(verdict))

def load_verdict(task_id: str) -> Optional[dict]:
    raw = get_redis().hget("judge:verdicts", task_id)
    return json.loads(raw) if raw else None
