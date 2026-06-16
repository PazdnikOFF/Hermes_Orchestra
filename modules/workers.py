"""
modules/workers.py — все рабочие процессы оркестра.

Каждый класс — одна роль. Запускается в отдельном процессе или потоке.
Общение только через Redis Streams (redis_bus.py).

ИНТЕГРИРОВАННЫЕ МОДУЛИ (итерация 3):
  soul_registry.py     — динамический каталог душ (Senior использует для планирования)
  soul_gap_resolver.py — запрос новых душ у пользователя при нехватке
  soul_evolver.py      — обратная запись улучшений в YAML после Judge
  task_resilience.py   — retry, DLQ, heartbeat, делегирование при падении

ПРАВКИ ДЛЯ СЛЕДУЮЩЕЙ ИТЕРАЦИИ:
  - Изменить логику Senior → SeniorWorker._handle()
  - Изменить логику автоскейлинга → WatchdogWorker._check_direction()
  - Изменить retry поведение → task_resilience.py
  - Изменить эволюцию душ → soul_evolver.py
"""

from __future__ import annotations

import json
import logging
import os
import signal
import statistics
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from modules.models import AgentRole, AgentState, Task, TaskStatus, make_agent_id
from modules.redis_bus import (
    AgentRegistry, STREAM_SENIOR, STREAM_JUDGE_IN,
    ack_task, publish_task, read_tasks, save_result, load_result,
    stream_agent, stream_middle, get_redis,
)
from modules.llm_bridge import (
    agent_call, assemble_final_result, doubt_agent_review,
    judge_evaluate, maybe_evolve_skill, middle_plan_subtask,
    parse_agent_output, senior_plan_task,
)
from modules.task_resilience import (
    heartbeat_write, assign_task, clear_assignment,
    schedule_retry, ResilienceWatcher,
)
from modules import config as cfg_module

if TYPE_CHECKING:
    from modules.agent_factory import AgentFactory
    from modules.soul_registry import SoulRegistry
    from modules.soul_gap_resolver import SoulGapResolver
    from modules.soul_evolver import SoulEvolver
    from modules.result_assembler import ResultAssembler

log = logging.getLogger("orchestra.workers")

# Redis-ключ для PID-ов воркеров, запущенных динамически (Junior/Agent/Clone).
# orchestra_ctl stop читает их вместе со списком стартовых процессов.
DYNAMIC_PIDS_KEY = "orchestra:dynamic_pids"


def _worker_lock_key(kind: str, direction: str) -> str:
    """Redis-ключ блокировки для дедупликации subprocess'ов."""
    return f"worker:running:{kind}:{direction}"


WORKER_LOCK_TTL = 120  # секунд — воркер должен рефрешить чаще


def _ensure_worker_running(kind: str, direction: str, worker_id: str) -> bool:
    """
    Идемпотентный запуск subprocess. SETNX-lock с TTL гарантирует один
    живой процесс на (kind, direction).

    Воркер сам обновляет lock в основном цикле; если умирает — lock
    истекает за WORKER_LOCK_TTL и следующий вызов запустит новый.

    Возвращает True если spawn состоялся, False если другой уже живёт.
    """
    try:
        from modules.redis_bus import get_redis
        r = get_redis()
        if not r.set(_worker_lock_key(kind, direction), worker_id,
                     nx=True, ex=WORKER_LOCK_TTL):
            log.debug("[ensure] %s/%s уже запущен — skip spawn", kind, direction)
            return False
    except Exception as exc:
        log.warning("[ensure] не удалось взять lock: %s — спавним всё равно", exc)

    _spawn_worker_subprocess(kind, worker_id, direction)
    return True


def _refresh_worker_lock(kind: str, direction: str, worker_id: str) -> None:
    """Воркер обновляет свой lock в основном цикле."""
    try:
        from modules.redis_bus import get_redis
        get_redis().setex(_worker_lock_key(kind, direction), WORKER_LOCK_TTL, worker_id)
    except Exception:
        pass


def _spawn_worker_subprocess(kind: str, worker_id: str, direction: str = "") -> Optional[int]:
    """
    Запускает воркер в отдельном subprocess.

    kind ∈ {"agent", "junior", "middle"} — диспатчится в __main__ ниже.
    Subprocess отвязан от родителя (start_new_session) — переживает рестарт.
    PID пишется в Redis для cmd_stop.
    """
    cmd = [sys.executable, "-m", "modules.workers", kind, worker_id]
    if direction:
        cmd.append(direction)
    env = os.environ.copy()
    pkg_root = str(Path(__file__).parent.parent)
    env["PYTHONPATH"] = pkg_root + os.pathsep + env.get("PYTHONPATH", "")
    # Force unbuffered IO для немедленных логов
    env["PYTHONUNBUFFERED"] = "1"

    # Логи subprocess'ов — в общий файл (append). Раньше шли в DEVNULL и
    # все падения были невидимы.
    log_dir = Path(env.get("ORCHESTRA_LOG_DIR",
                           os.path.expanduser("~/.hermes")))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "orchestra-workers.log"
    try:
        log_fh = open(log_path, "ab")
        header = f"\n--- {time.strftime('%H:%M:%S')} spawn {kind}/{worker_id} dir={direction} ---\n"
        log_fh.write(header.encode())
        log_fh.flush()
    except Exception:
        log_fh = subprocess.DEVNULL  # type: ignore

    try:
        proc = subprocess.Popen(
            cmd, env=env, cwd=pkg_root,
            stdout=log_fh, stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except Exception as exc:
        log.error("[spawn] не удалось запустить %s/%s: %s", kind, worker_id, exc)
        return None

    log.info("[spawn] %s/%s → pid=%d", kind, worker_id, proc.pid)
    try:
        from modules.redis_bus import get_redis
        get_redis().sadd(DYNAMIC_PIDS_KEY, str(proc.pid))
        # Дополнительно регистрируем PID для teardown по (kind, direction)
        if direction:
            from modules.agent_lifecycle import register_worker_pid
            register_worker_pid(kind, direction, proc.pid)
    except Exception as exc:
        log.warning("[spawn] не удалось записать pid в Redis: %s", exc)
    return proc.pid


# ── DAG: граф зависимостей направлений (итерация 7) ─────────────────────────
# Senior может выдать план с depends_on. Корни публикуются сразу, остальные —
# по мере завершения предков (dag_advance из Judge/Watchdog), с пробросом
# результатов предков в контекст. Публикация идемпотентна (SADD-гейт).

DAG_PLAN_KEY       = "dag:{}:plan"
DAG_PUBLISHED_KEY  = "dag:{}:published"
DAG_PENDING_SUFFIX = "_PENDING"
DAG_PLAN_TTL       = 86400          # сутки — страховка от утечки ключей
_DAG_TERMINAL      = {"done", "dlq", "failed", "partial_dlq"}


def _dag_placeholder(parent_id: str, direction: str) -> str:
    """Псевдо-id направления в decomposition до его публикации.
    Никогда не терминален → держит ResultAssembler от ранней сборки."""
    return f"{parent_id}_{direction}{DAG_PENDING_SUFFIX}"


def _dag_direction_done(r, ids: list[str]) -> bool:
    """Направление завершено: есть реальные id (не placeholder) и все терминальны."""
    if not ids or any(sid.endswith(DAG_PENDING_SUFFIX) for sid in ids):
        return False
    return all(r.hget(f"results:{sid}", "status") in _DAG_TERMINAL for sid in ids)


def _dag_gather_upstream(r, decomp: dict, deps: list[str]) -> str:
    """Собирает результаты направлений-предков для проброса в контекст потомка."""
    parts = []
    for dep in deps:
        for sid in decomp.get(dep, []):
            if sid.endswith(DAG_PENDING_SUFFIX):
                continue
            res = r.hget(f"results:{sid}", "result")
            if res:
                parts.append(f"### {dep}\n{res}")
    return "\n\n".join(parts)


def _dag_replace_placeholder(parent_id: str, direction: str, real_ids: list[str]) -> None:
    """Атомарно заменяет placeholder направления в decomposition на реальные id."""
    r = get_redis()
    key = f"results:{parent_id}"
    for _ in range(5):
        with r.pipeline() as pipe:
            try:
                pipe.watch(key)
                raw = pipe.hget(key, "decomposition")
                decomp = json.loads(raw) if raw else {}
                decomp[direction] = real_ids
                pipe.multi()
                pipe.hset(key, "decomposition", json.dumps(decomp))
                pipe.execute()
                return
            except Exception:
                continue


def _dag_publish_direction(parent_id, plan, direction, factory, source_id,
                           retry_max=3):
    """
    Публикует одно направление (идемпотентно через SADD). Подмешивает
    результаты направлений-предков в контекст подзадачи.

    ВАЖНО: сначала заменяем placeholder на реальные id в decomposition,
    и ТОЛЬКО потом публикуем в Middle — иначе MiddleWorker._patch_parent_
    decomposition не найдёт id для замены на mt-айдишники (риск зависания).
    """
    r = get_redis()
    # exactly-once: публикует лишь тот, кто первым добавил направление в set
    if not r.sadd(DAG_PUBLISHED_KEY.format(parent_id), direction):
        return
    spec = next((s for s in plan if s["direction"] == direction), None)
    if not spec:
        return

    subtask = spec.get("subtask") or load_result(parent_id).get("original_task", "")
    count   = int(spec.get("count", 1) or 1)
    deps    = spec.get("depends_on", [])

    content = subtask
    if deps:
        try:
            decomp = json.loads(load_result(parent_id).get("decomposition", "{}"))
        except Exception:
            decomp = {}
        ctx = _dag_gather_upstream(r, decomp, deps)
        if ctx:
            content = (
                subtask
                + "\n\n--- Результаты предыдущих направлений (используй как исходные данные) ---\n"
                + ctx
            )

    from modules.agent_lifecycle import acquire_direction
    acquire_direction(parent_id, direction)
    middle = factory.create_middle(direction)
    _ensure_worker_running("middle", direction, middle.id)

    real_ids = [f"{parent_id}_{direction}_{uuid.uuid4().hex[:6]}" for _ in range(count)]
    _dag_replace_placeholder(parent_id, direction, real_ids)
    for sid in real_ids:
        sub = Task(
            id=sid, content=content, direction=direction,
            source_agent_id=source_id, retry_max=retry_max,
            parent_task_id=parent_id,
        )
        publish_task(stream_middle(direction), sub)
        log.info("[DAG] %s → Middle/%s: %s (deps=%s)", parent_id, direction, sid, deps)


def dag_advance(parent_id, factory, source_id="judge-dag"):
    """
    Вызывается после завершения сабтаска: публикует направления, чьи
    зависимости теперь выполнены. Идемпотентно, без блокировок (гейт — SADD).
    Для не-DAG задач (нет ключа dag:*:plan) — no-op.
    """
    if not parent_id:
        return
    r = get_redis()
    plan_raw = r.get(DAG_PLAN_KEY.format(parent_id))
    if not plan_raw:
        return
    try:
        plan = json.loads(plan_raw)
        decomp = json.loads(load_result(parent_id).get("decomposition", "{}"))
    except Exception:
        return

    done = {d for d, ids in decomp.items() if _dag_direction_done(r, ids)}
    published = set(r.smembers(DAG_PUBLISHED_KEY.format(parent_id)))

    from modules.dag import ready_directions
    for d in ready_directions(plan, done, published):
        try:
            _dag_publish_direction(parent_id, plan, d, factory, source_id)
        except Exception as exc:
            log.exception("[DAG] publish %s/%s failed: %s", parent_id, d, exc)


# ── Base ──────────────────────────────────────────────────────────────────

class BaseWorker:
    def __init__(self, worker_id: str, factory: "AgentFactory"):
        self.worker_id = worker_id
        self.factory   = factory
        self._stop     = False
        signal.signal(signal.SIGTERM, self._on_stop)
        signal.signal(signal.SIGINT,  self._on_stop)

    def _on_stop(self, *_):
        # ВАЖНО: внутри signal handler НЕЛЬЗЯ звать log.* — если сигнал
        # пришёл во время write на stderr, Python ругается RuntimeError:
        # reentrant call inside <_io.BufferedWriter>. Просто флаг.
        self._stop = True

    def run(self):
        raise NotImplementedError


# ── Senior ────────────────────────────────────────────────────────────────

class SeniorWorker(BaseWorker):
    """
    Единственный бессмертный оркестратор.

    НОВОЕ В ИТЕРАЦИИ 3:
    - Использует SoulRegistry для объективной оценки доступных душ
    - Использует SoulGapResolver когда нужные направления отсутствуют
    - Паркует задачи пока пользователь не предоставит нужные души
    """

    GROUP = "senior_group"

    def __init__(self, worker_id: str, factory: "AgentFactory",
                 soul_registry: "SoulRegistry",
                 gap_resolver: "SoulGapResolver"):
        super().__init__(worker_id, factory)
        self.soul_registry = soul_registry
        self.gap_resolver  = gap_resolver

    def run(self):
        log.info("[Senior] запущен")
        while not self._stop:
            entries = read_tasks(STREAM_SENIOR, self.GROUP, self.worker_id)
            for entry_id, task in entries:
                try:
                    self._handle(task)
                    ack_task(STREAM_SENIOR, self.GROUP, entry_id)
                except Exception as exc:
                    log.exception("[Senior] ошибка задачи %s: %s", task.id, exc)
                    ack_task(STREAM_SENIOR, self.GROUP, entry_id)

    def _handle(self, task: Task):
        log.info("[Senior] декомпозиция задачи %s", task.id)

        # ── 1. Перестраиваем индекс если устарел ──────────────────────────
        if self.soul_registry.is_stale():
            log.info("[Senior] индекс устарел → rebuild")
            self.soul_registry.rebuild(use_llm=False)

        # ── 2. Планирование: объективная оценка нужных направлений ────────
        # Передаём фактический список доступных направлений как контекст
        # (LLM может выходить за его пределы — это не ограничение, а hint).
        available = sorted({
            e.direction for e in self.soul_registry.get_all(role_type="agent")
        })
        plan = senior_plan_task(task.content, available_directions=available)

        # ── 3. Проверяем: есть ли нужные души в SOULs/ ────────────────────
        covered_plan, gap_requests = self.gap_resolver.check_and_request(
            task.id, task.content, plan
        )

        # ── 4. Если есть дефицит — уведомляем пользователя ────────────────
        if gap_requests:
            notice = self.gap_resolver.notify_user(gap_requests)
            save_result(task.id, "gap_notice", notice)
            save_result(task.id, "gap_count", str(len(gap_requests)))
            log.warning("[Senior] %d дефицитных направлений для задачи %s",
                        len(gap_requests), task.id)
            print(notice)  # виден в консоли / Hermes логах

        # ── 5. Запускаем то что можно прямо сейчас ────────────────────────
        if not covered_plan:
            # Полностью заблокированная задача → парковка + ждём gap-resolve.
            # Партиал не паркуем чтобы не задублировать сабтаски при republish.
            save_result(task.id, "status", "waiting_for_souls")
            save_result(task.id, "original_task", task.content)
            self.gap_resolver.park_task(task)
            log.info("[Senior] задача %s заблокирована — нет ни одной подходящей души",
                     task.id)
            return

        # ── Граф зависимостей (итерация 7) или плоский параллельный режим ──
        from modules.dag import normalize_plan, has_dependencies, is_acyclic
        plan = normalize_plan(covered_plan)
        if has_dependencies(plan):
            if is_acyclic(plan):
                self._handle_dag(task, plan, bool(gap_requests))
                return
            log.warning("[Senior] %s: план с зависимостями содержит цикл — "
                        "fallback на параллельный режим", task.id)

        # Плоский режим: все направления стартуют параллельно (как раньше).
        subtask_map: dict[str, list[str]] = {}
        for spec in plan:
            direction = spec["direction"]
            subtask   = spec["subtask"] or task.content
            count     = spec["count"]

            # Refcount: задача "арендует" direction; ResultAssembler
            # после финала отпускает и убивает агентов когда refcount=0.
            from modules.agent_lifecycle import acquire_direction
            acquire_direction(task.id, direction)

            middle = self.factory.create_middle(direction)
            _ensure_worker_running("middle", direction, middle.id)

            for _ in range(count):
                sub = Task(
                    id=f"{task.id}_{direction}_{uuid.uuid4().hex[:6]}",
                    content=subtask,
                    direction=direction,
                    source_agent_id=self.worker_id,
                    retry_max=task.retry_max,
                    parent_task_id=task.id,
                )
                publish_task(stream_middle(direction), sub)
                subtask_map.setdefault(direction, []).append(sub.id)
                log.info("[Senior] → Middle/%s: %s", direction, sub.id)

        save_result(task.id, "decomposition", json.dumps(subtask_map))
        save_result(task.id, "original_task",  task.content)
        save_result(task.id, "status",
                    "partial" if gap_requests else "decomposed")

    def _handle_dag(self, task: Task, plan: list[dict], partial: bool) -> None:
        """
        Запуск задачи в режиме графа зависимостей.
        decomposition заполняется placeholder'ами для ВСЕХ направлений (чтобы
        ResultAssembler ждал их все), публикуются только корни (без depends_on).
        Остальные направления освобождает dag_advance по мере готовности предков.
        """
        from modules.dag import ready_directions

        decomp = {s["direction"]: [_dag_placeholder(task.id, s["direction"])]
                  for s in plan}
        save_result(task.id, "decomposition", json.dumps(decomp))
        save_result(task.id, "original_task", task.content)
        save_result(task.id, "status", "partial" if partial else "decomposed")

        r = get_redis()
        r.set(DAG_PLAN_KEY.format(task.id), json.dumps(plan), ex=DAG_PLAN_TTL)
        r.delete(DAG_PUBLISHED_KEY.format(task.id))

        roots = ready_directions(plan, done_dirs=set(), published_dirs=set())
        log.info("[Senior] %s DAG-режим: %d направлений, корни=%s",
                 task.id, len(plan), roots)
        for d in roots:
            _dag_publish_direction(task.id, plan, d, self.factory,
                                   self.worker_id, retry_max=task.retry_max)


# ── Middle ────────────────────────────────────────────────────────────────

class MiddleWorker(BaseWorker):
    GROUP_PREFIX = "middle"

    def __init__(self, worker_id: str, factory: "AgentFactory", direction: str):
        super().__init__(worker_id, factory)
        self.direction = direction
        self.stream    = stream_middle(direction)
        self.group     = f"{self.GROUP_PREFIX}_{direction}"

    def run(self):
        log.info("[Middle/%s] запущен", self.direction)
        while not self._stop:
            _refresh_worker_lock("middle", self.direction, self.worker_id)
            # Диспетчер: батчим чтение. Задачи обрабатываются последовательно
            # в цикле, каждая ack'ается отдельно — consumer group гарантирует
            # доставку ровно одному воркеру, дублей нет.
            entries = read_tasks(self.stream, self.group, self.worker_id, count=10)
            for entry_id, task in entries:
                try:
                    self._handle(task)
                    ack_task(self.stream, self.group, entry_id)
                except Exception as exc:
                    log.exception("[Middle/%s] ошибка %s: %s", self.direction, task.id, exc)
                    ack_task(self.stream, self.group, entry_id)

    def _handle(self, task: Task):
        micro_tasks = middle_plan_subtask(self.direction, task.content)
        junior      = self.factory.create_junior(self.direction, self.worker_id)
        _ensure_worker_running("junior", self.direction, junior.id)

        mt_ids = [f"{task.id}_mt{i}" for i in range(len(micro_tasks))]

        # КРИТИЧНО: обновляем parent decomposition mt-айдишниками вместо
        # placeholder'а с Middle-уровневым ID. Без этого ResultAssembler
        # будет ждать статус, который никогда не наступит.
        if task.parent_task_id:
            self._patch_parent_decomposition(task.parent_task_id, task.id, mt_ids)

        junior_stream = f"tasks:junior:{self.direction}"
        for i, spec in enumerate(micro_tasks):
            mt = Task(
                id=mt_ids[i],
                content=spec.get("micro_task", task.content),
                direction=self.direction,
                source_agent_id=self.worker_id,
                retry_max=task.retry_max,
                parent_task_id=task.parent_task_id or task.id,
            )
            publish_task(junior_stream, mt)

    def _patch_parent_decomposition(
        self, parent_task_id: str, old_id: str, new_ids: list[str]
    ) -> None:
        """Замена placeholder'а Middle-уровня на список mt-IDs.
        Атомарно через WATCH/MULTI — на случай если два Middle бьют в одну
        корневую задачу параллельно."""
        r = get_redis()
        key = f"results:{parent_task_id}"
        for _ in range(5):  # ретрай при WATCH-конфликте
            with r.pipeline() as pipe:
                try:
                    pipe.watch(key)
                    raw = pipe.hget(key, "decomposition")
                    if not raw:
                        pipe.unwatch()
                        return
                    try:
                        decomp = json.loads(raw)
                    except json.JSONDecodeError:
                        pipe.unwatch()
                        return
                    changed = False
                    for direction, ids in decomp.items():
                        if old_id in ids:
                            ids.remove(old_id)
                            for nid in new_ids:
                                if nid not in ids:
                                    ids.append(nid)
                            changed = True
                            break
                    if not changed:
                        pipe.unwatch()
                        return
                    pipe.multi()
                    pipe.hset(key, "decomposition", json.dumps(decomp))
                    pipe.execute()
                    return
                except Exception:
                    continue


# ── Junior ────────────────────────────────────────────────────────────────

class JuniorWorker(BaseWorker):
    def __init__(self, worker_id: str, factory: "AgentFactory", direction: str):
        super().__init__(worker_id, factory)
        self.direction = direction
        self.stream    = f"tasks:junior:{direction}"
        self.group     = f"junior_{direction}"
        self._rr_idx   = 0

    def run(self):
        log.info("[Junior/%s] запущен", self.direction)
        while not self._stop:
            _refresh_worker_lock("junior", self.direction, self.worker_id)
            # Диспетчер: батчим чтение (см. MiddleWorker.run).
            entries = read_tasks(self.stream, self.group, self.worker_id, count=10)
            for entry_id, task in entries:
                try:
                    self._dispatch(task)
                    ack_task(self.stream, self.group, entry_id)
                except Exception as exc:
                    log.exception("[Junior/%s] ошибка %s: %s", self.direction, task.id, exc)
                    ack_task(self.stream, self.group, entry_id)

    def _dispatch(self, task: Task):
        agents = self._get_or_create_agents()
        agent  = agents[self._rr_idx % len(agents)]
        self._rr_idx += 1
        task.assigned_agent_id = agent.id
        assign_task(task, agent.id)
        publish_task(stream_agent(agent.id), task)
        log.info("[Junior/%s] → %s: %s", self.direction, agent.id, task.id)

    def _get_or_create_agents(self) -> list[AgentState]:
        agents = [
            a for a in AgentRegistry.by_direction(self.direction)
            if a.role in (AgentRole.AGENT, AgentRole.AGENT_ORCHESTRATOR)
        ]
        if not agents:
            agent = self.factory.create_agent(self.direction)
            agents = [agent]
        # Lock дедуплицирует: реальный spawn случится только если процесса нет
        for agent in agents:
            _ensure_worker_running("agent", self.direction, agent.id)
        return agents


# ── Agent ─────────────────────────────────────────────────────────────────

class AgentWorker(BaseWorker):
    """
    НОВОЕ В ИТЕРАЦИИ 3:
    - Пишет heartbeat каждые HEARTBEAT_INTERVAL секунд
    - При ошибке вызывает schedule_retry вместо простого логирования
    - Очищает assignment после завершения задачи
    """

    def __init__(self, worker_id: str, factory: "AgentFactory"):
        super().__init__(worker_id, factory)
        self.stream       = stream_agent(worker_id)
        self.group        = f"agent_{worker_id}"
        self._hb_thread: Optional[threading.Thread] = None

    def run(self):
        log.info("[Agent/%s] запущен", self.worker_id)
        # Heartbeat в отдельном демон-потоке. КРИТИЧНО: LLM-вызов в _execute
        # синхронный и может длиться до timeout (120с) — дольше HEARTBEAT_TTL
        # (30с). Если писать heartbeat только в рабочем цикле, во время долгого
        # вызова heartbeat протухает, ResilienceWatcher считает агента мёртвым
        # и переназначает задачу другому агенту → два агента на один task.id
        # → дубли результатов в Judge. Поток пишет heartbeat независимо от того,
        # занят ли рабочий цикл LLM-вызовом.
        self._hb_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._hb_thread.start()

        while not self._stop:
            agent = AgentRegistry.load(self.worker_id)
            if not agent:
                log.warning("[Agent/%s] не найден в реестре", self.worker_id)
                time.sleep(5)
                continue

            if agent.role == AgentRole.AGENT_ORCHESTRATOR:
                self._orchestrate(agent)
                continue

            entries = read_tasks(self.stream, self.group, self.worker_id, block_ms=2000)
            for entry_id, task in entries:
                t0 = time.time()
                try:
                    task.status     = TaskStatus.RUNNING
                    task.started_at = t0
                    self._execute(agent, task)
                    latency = (time.time() - t0) * 1000
                    AgentRegistry.update_metrics(self.worker_id, latency)
                    clear_assignment(task.id)
                    ack_task(self.stream, self.group, entry_id)
                except Exception as exc:
                    error_msg = str(exc)
                    log.exception("[Agent/%s] ошибка %s: %s", self.worker_id, task.id, exc)
                    ack_task(self.stream, self.group, entry_id)
                    # Retry вместо молчаливого проглатывания
                    schedule_retry(task, error_msg, self.stream,
                                   exclude_agent_id=self.worker_id)

    def _heartbeat_loop(self):
        """Фоновый поток: пишет heartbeat и рефрешит worker-lock независимо
        от рабочего цикла. См. комментарий в run()."""
        from modules.task_resilience import HEARTBEAT_INTERVAL
        while not self._stop:
            try:
                heartbeat_write(self.worker_id)
                # Заодно рефрешим worker-lock чтобы Junior не плодил дубликаты
                agent = AgentRegistry.load(self.worker_id)
                if agent:
                    _refresh_worker_lock("agent", agent.direction, self.worker_id)
            except Exception:
                # heartbeat не должен ронять воркер — глушим и пробуем снова
                pass
            # Дробный сон чтобы быстро реагировать на _stop
            for _ in range(HEARTBEAT_INTERVAL):
                if self._stop:
                    return
                time.sleep(1)

    def _execute(self, agent: AgentState, task: Task):
        # Если это rework — передаём критику Judge как extra_context для агента
        extra = ""
        if task.rework_context:
            max_iter = cfg_module.CFG.judge_max_iterations
            extra = (
                f"⚠ ДОРАБОТКА (попытка {task.judge_iteration}/{max_iter}). "
                f"Судья оценил твой предыдущий результат ниже порога качества "
                f"и просит исправить:\n{task.rework_context}\n"
                "Учти эту критику и выдай улучшенный результат."
            )

        raw    = agent_call(agent, task.content, extra_context=extra)
        parsed = parse_agent_output(raw)
        result = parsed.get("result", raw)
        learned = parsed.get("learned")

        judge_task = Task(
            id=task.id, content=task.content,
            direction=task.direction,
            source_agent_id=self.worker_id,
            status=TaskStatus.VALIDATING,
            result=result,
            retry_count=task.retry_count,
            retry_max=task.retry_max,
            parent_task_id=task.parent_task_id,
            judge_iteration=task.judge_iteration,
        )
        # Сначала отдаём результат в Judge — критический путь латентности.
        publish_task(STREAM_JUDGE_IN, judge_task)

        # Эволюция скилла — ПОСЛЕ publish: она каждые N задач делает лишний
        # синхронный LLM-вызов, который не должен задерживать выдачу результата.
        # Остаётся в этом же процессе (сериализовано) — без гонки записи в
        # agent:{id}:state, которая возникла бы при выносе в отдельный воркер.
        maybe_evolve_skill(agent, learned)

    def _orchestrate(self, agent: AgentState):
        entries = read_tasks(self.stream, self.group, self.worker_id, block_ms=2000)
        for entry_id, task in entries:
            clones = agent.clone_ids
            if not clones:
                agent.role = AgentRole.AGENT
                AgentRegistry.save(agent)
                log.info("[Agent/%s] 0 клонов → AGENT", self.worker_id)
                ack_task(self.stream, self.group, entry_id)
                return
            target = clones[hash(task.id) % len(clones)]
            publish_task(stream_agent(target), task)
            ack_task(self.stream, self.group, entry_id)
        time.sleep(0.1)


# ── Judge ─────────────────────────────────────────────────────────────────

class JudgeWorker(BaseWorker):
    """
    НОВОЕ В ИТЕРАЦИИ 3:
    - После passed=True вызывает SoulEvolver.maybe_evolve()
      для записи улучшений обратно в soul.yaml

    НОВОЕ В ИТЕРАЦИИ 4:
    - После сохранения статуса done вызывает ResultAssembler.maybe_assemble
      по task.parent_task_id — если все сабтаски готовы, собирает финал
    """

    GROUP = "judge_group"

    def __init__(self, worker_id: str, factory: "AgentFactory",
                 soul_evolver: "SoulEvolver",
                 result_assembler: "ResultAssembler"):
        super().__init__(worker_id, factory)
        self.soul_evolver     = soul_evolver
        self.result_assembler = result_assembler

    def run(self):
        log.info("[Judge] запущен")
        while not self._stop:
            entries = read_tasks(STREAM_JUDGE_IN, self.GROUP, self.worker_id)
            for entry_id, task in entries:
                try:
                    self._evaluate(task)
                    ack_task(STREAM_JUDGE_IN, self.GROUP, entry_id)
                except Exception as exc:
                    log.exception("[Judge] ошибка %s: %s", task.id, exc)
                    ack_task(STREAM_JUDGE_IN, self.GROUP, entry_id)

    def _evaluate(self, task: Task):
        agent       = AgentRegistry.load(task.source_agent_id)
        soul_prompt = agent.soul.to_prompt() if agent else "(unknown)"

        verdict = judge_evaluate(task.direction, task.content, task.result or "", soul_prompt)
        score   = float(verdict.get("score", 0.0))
        crit_llm = verdict.get("critique", "")

        # Опционально — doubt-агент добавляет дополнительную критику
        doubt_critique = ""
        if verdict.get("needs_doubt_agent"):
            log.info("[Judge] создаём doubt-агента для %s", task.id)
            doubt_agent = self.factory.create_doubt_agent(task.direction, soul_prompt)
            try:
                doubt_critique = doubt_agent_review(
                    task.direction, task.content, task.result or "",
                    verdict.get("doubt_focus", ""),
                    self.factory.loader.get_judge_doubt_overlay(),
                )
            finally:
                self.factory.delete_agent(doubt_agent.id)

        combined_critique = "\n".join(p for p in (crit_llm, doubt_critique) if p)

        # ── Сохраняем попытку в history (для best-of при лимите) ──────────
        self._append_history(task.id, {
            "attempt":  task.judge_iteration,
            "score":    score,
            "verdict":  verdict.get("verdict", ""),
            "critique": combined_critique,
            "result":   task.result or "",
            "agent_id": task.source_agent_id,
            "ts":       time.time(),
        })

        threshold = cfg_module.CFG.judge_pass_threshold     # 0.79
        max_iter  = cfg_module.CFG.judge_max_iterations     # 5
        passed    = score > threshold                       # > 0.79

        log.info("[Judge] %s — attempt=%d score=%.2f passed=%s",
                 task.id, task.judge_iteration, score, passed)

        # ── Rework: score низкий и лимит не исчерпан ──────────────────────
        if not passed and task.judge_iteration < max_iter:
            self._send_for_rework(task, combined_critique, score)
            return

        # ── Финализация: либо passed, либо лимит исчерпан ─────────────────
        final_result_text = task.result or ""
        used_best = False
        best = None
        if not passed and task.judge_iteration >= max_iter:
            best = self._pick_best(task.id)
            if best:
                final_result_text = best.get("result", final_result_text)
                score = best.get("score", score)
                used_best = True
                log.warning(
                    "[Judge] %s — лимит %d итераций. Выбран лучший: "
                    "score=%.2f attempt=%d",
                    task.id, max_iter, score, best.get("attempt", -1),
                )

        full_verdict = {
            "passed":         passed,
            "score":          score,
            "verdict":        verdict.get("verdict", ""),
            "critique":       combined_critique,
            "attempts":       task.judge_iteration + 1,
            "max_attempts":   max_iter,
            "finalized_as":   "passed" if passed else ("best_of" if used_best else "exhausted"),
        }

        from modules.redis_bus import save_result, save_verdict
        save_verdict(task.id, full_verdict)
        save_result(task.id, "result",     final_result_text)
        save_result(task.id, "verdict",    json.dumps(full_verdict))
        save_result(task.id, "score",      f"{score:.4f}")
        save_result(task.id, "attempts",   str(task.judge_iteration + 1))
        save_result(task.id, "status",     "done")

        # ── Эволюция души только при честном passed ───────────────────────
        if passed and agent:
            history = AgentRegistry.get_skill_history(task.source_agent_id, last_n=1)
            learned = history[-1].get("learned") if history else None
            evolved = self.soul_evolver.maybe_evolve(
                direction=agent.direction,
                role_type="agent",
                task_content=task.content,
                result_text=final_result_text,
                learned=learned,
                verdict=full_verdict,
            )
            if evolved:
                log.info("[Judge] душа агента %s улучшена", agent.direction)

        # ── DAG: публикуем направления, чьи зависимости теперь выполнены ──
        try:
            dag_advance(task.parent_task_id, self.factory)
        except Exception as exc:
            log.exception("[Judge] DAG advance error %s: %s",
                          task.parent_task_id, exc)

        # ── Авто-сборка финала ────────────────────────────────────────────
        try:
            if self.result_assembler.maybe_assemble(task.parent_task_id):
                log.info("[Judge] финал собран для %s", task.parent_task_id)
        except Exception as exc:
            log.exception("[Judge] ошибка авто-сборки %s: %s",
                          task.parent_task_id, exc)

        # ── Чистим history (опционально, после успеха) ────────────────────
        # Оставляем history на 1 час для диагностики через CLI.
        get_redis().expire(f"judge:history:{task.id}", 3600)

    # ── Rework helpers ─────────────────────────────────────────────────────

    def _send_for_rework(self, task: Task, critique: str, score: float) -> None:
        """Переотправляет задачу исходному агенту с rework_context."""
        next_iter = task.judge_iteration + 1
        rework = Task(
            id=task.id,
            content=task.content,
            direction=task.direction,
            source_agent_id="judge",
            status=TaskStatus.RETRYING,
            retry_count=task.retry_count,
            retry_max=task.retry_max,
            parent_task_id=task.parent_task_id,
            judge_iteration=next_iter,
            rework_context=(critique or "")[:1500],
            assigned_agent_id=task.source_agent_id,
        )

        target_agent_id = task.source_agent_id
        target_stream   = stream_agent(target_agent_id)
        # Отметка для ResilienceWatcher
        assign_task(rework, target_agent_id)
        publish_task(target_stream, rework)
        log.info(
            "[Judge] %s → rework #%d (score=%.2f) к агенту %s",
            task.id, next_iter, score, target_agent_id,
        )

    @staticmethod
    def _history_key(task_id: str) -> str:
        return f"judge:history:{task_id}"

    def _append_history(self, task_id: str, entry: dict) -> None:
        r = get_redis()
        key = self._history_key(task_id)
        r.rpush(key, json.dumps(entry))
        r.expire(key, 86400)  # 1 день — для диагностики

    def _pick_best(self, task_id: str) -> Optional[dict]:
        r = get_redis()
        raw = r.lrange(self._history_key(task_id), 0, -1)
        if not raw:
            return None
        history = []
        for x in raw:
            try:
                history.append(json.loads(x))
            except Exception:
                pass
        if not history:
            return None
        return max(history, key=lambda h: h.get("score", 0.0))


# ── Watchdog ─────────────────────────────────────────────────────────────

class WatchdogWorker(BaseWorker):
    """
    НОВОЕ В ИТЕРАЦИИ 3:
    - Запускает ResilienceWatcher для обнаружения мёртвых агентов
    """

    def run(self):
        interval = cfg_module.CFG.watchdog_interval
        log.info("[Watchdog] запущен (интервал=%ds)", interval)
        watcher = ResilienceWatcher()

        # ── Startup recovery (TODO 5) ─────────────────────────────────────
        # Ждём grace-период, чтобы существующие AgentWorker успели
        # записать первый heartbeat. После этого восстанавливаем подвисшие
        # с прошлого запуска задачи (assigned_at > 2× HEARTBEAT_TTL).
        from modules.task_resilience import HEARTBEAT_TTL
        grace = HEARTBEAT_TTL + 5
        log.info("[Watchdog] startup grace %ds перед recovery…", grace)
        for _ in range(grace):
            if self._stop:
                return
            time.sleep(1)
        try:
            recovered = watcher.recover_on_startup()
            if recovered:
                log.warning("[Watchdog] startup recovery: %d задач переотправлено",
                            recovered)
        except Exception as exc:
            log.exception("[Watchdog] startup recovery failed: %s", exc)

        while not self._stop:
            try:
                self._tick()
                self._dag_tick()
                reassigned = watcher.check_all()
                if reassigned:
                    log.info("[Watchdog] переназначено задач: %d", reassigned)
            except Exception as exc:
                log.exception("[Watchdog] ошибка тика: %s", exc)
            time.sleep(interval)

    def _dag_tick(self):
        """Страховка для DAG: периодически двигаем графы вперёд. Закрывает
        случай, когда направление-предок ушло в DLQ без Judge-события — тогда
        потомки не разблокировались бы по обычному хуку из JudgeWorker."""
        r = get_redis()
        for key in r.scan_iter(match="dag:*:plan", count=100):
            parent_id = key.split(":")[1]
            try:
                dag_advance(parent_id, self.factory)
            except Exception:
                pass

    def _tick(self):
        from modules.redis_bus import get_redis
        r = get_redis()
        directions = set(r.hvals("orchestra:agents"))
        for d in directions:
            if d not in ("senior", "middle", "junior", "judge"):
                self._check_direction(d)

    def _check_direction(self, direction: str):
        from modules.redis_bus import get_redis
        r = get_redis()
        members = r.zrange(f"metrics:latency:{direction}", 0, -1, withscores=True)
        if len(members) < 2:
            return
        latencies = [s for _, s in members if s > 0]
        if not latencies:
            return
        median    = statistics.median(latencies)
        threshold = median * cfg_module.CFG.slow_threshold

        for agent_id, score in members:
            if score > threshold:
                agent = AgentRegistry.load(agent_id)
                if agent and agent.role == AgentRole.AGENT and not agent.parent_agent_id:
                    self._promote_and_clone(agent)

        self._gc_idle_clones(direction)

    def _promote_and_clone(self, agent: AgentState):
        if agent.clone_ids:
            return
        cfg = cfg_module.CFG
        n   = min(3, cfg.max_clones)
        log.info("[Watchdog] медленный агент %s → клонируем x%d", agent.id, n)
        for _ in range(n):
            clone = self.factory.create_clone(agent)
            # Клон-воркер в отдельном процессе (TODO 3)
            _spawn_worker_subprocess("agent", clone.id)
        agent = AgentRegistry.load(agent.id)
        agent.role = AgentRole.AGENT_ORCHESTRATOR
        AgentRegistry.save(agent)
        log.info("[Watchdog] %s → AGENT_ORCHESTRATOR (%d клонов)", agent.id, len(agent.clone_ids))

    def _gc_idle_clones(self, direction: str):
        now = time.time()
        ttl = cfg_module.CFG.idle_ttl
        for agent in AgentRegistry.by_direction(direction):
            if agent.role != AgentRole.AGENT or not agent.parent_agent_id:
                continue
            if agent.last_active_at and (now - agent.last_active_at) > ttl:
                log.info("[Watchdog] удаляем idle-клон %s", agent.id)
                parent = AgentRegistry.load(agent.parent_agent_id)
                if parent:
                    parent.clone_ids = [c for c in parent.clone_ids if c != agent.id]
                    if not parent.clone_ids:
                        parent.role = AgentRole.AGENT
                        log.info("[Watchdog] %s → AGENT (0 клонов)", parent.id)
                    AgentRegistry.save(parent)
                AgentRegistry.delete(agent.id)


# ── Subprocess entry point (для _spawn_worker_subprocess) ─────────────────
# Запускается так: python -m modules.workers <kind> <worker_id> [direction]
# kind ∈ {"agent", "junior", "middle"}.

def _subprocess_main() -> int:
    logging.basicConfig(
        level=os.environ.get("ORCHESTRA_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    if len(sys.argv) < 3:
        print("usage: python -m modules.workers <agent|junior|middle> <id> [direction]",
              file=sys.stderr)
        return 2

    kind      = sys.argv[1]
    worker_id = sys.argv[2]
    direction = sys.argv[3] if len(sys.argv) > 3 else ""

    # Импортируем factory лениво, чтобы избежать тяжёлой загрузки при импорте
    from modules.soul_loader import SoulLoader
    from modules.agent_factory import AgentFactory
    from modules import config as cfg_module
    factory = AgentFactory(SoulLoader(cfg_module.CFG.souls_dir))

    try:
        if kind == "agent":
            AgentWorker(worker_id, factory).run()
        elif kind == "junior":
            if not direction:
                print("junior requires direction", file=sys.stderr)
                return 2
            JuniorWorker(worker_id, factory, direction).run()
        elif kind == "middle":
            if not direction:
                print("middle requires direction", file=sys.stderr)
                return 2
            MiddleWorker(worker_id, factory, direction).run()
        else:
            print(f"unknown kind: {kind}", file=sys.stderr)
            return 2
    finally:
        # Снимаем себя из реестра живых PID-ов
        try:
            from modules.redis_bus import get_redis
            get_redis().srem(DYNAMIC_PIDS_KEY, str(os.getpid()))
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(_subprocess_main())
