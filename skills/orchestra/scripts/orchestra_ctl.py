#!/usr/bin/env python3
"""
orchestra_ctl.py — управление оркестром из командной строки.

Точка входа для всех операций. Не содержит бизнес-логики —
только вызывает нужные модули.

ПРАВКИ ДЛЯ СЛЕДУЮЩЕЙ ИТЕРАЦИИ:
  - Добавить новую команду → добавь функцию cmd_<name>(args) + регистрацию в main()
  - Изменить список воркеров при старте → редактируй cmd_start()
  - Добавить новый агент → просто создай SOULs/agents/<name>/soul.yaml,
    bootstrap автоматически подхватит новое направление
"""

from __future__ import annotations

import argparse
import json
import logging
import multiprocessing
import os
import sys
import time
import uuid
from pathlib import Path

# ── Пути ─────────────────────────────────────────────────────────────────
# Добавляем корень пакета в sys.path чтобы импорты modules.* работали
PACKAGE_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PACKAGE_ROOT))

# ── Логирование ───────────────────────────────────────────────────────────
logging.basicConfig(
    level=os.environ.get("ORCHESTRA_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("orchestra.ctl")

# ── PID файл ──────────────────────────────────────────────────────────────
PID_FILE = Path.home() / ".hermes" / "orchestra_pids.json"


def _get_factory():
    from modules.soul_loader import SoulLoader
    from modules.agent_factory import AgentFactory
    from modules import config as cfg_module
    loader = SoulLoader(cfg_module.CFG.souls_dir)
    return AgentFactory(loader)


def _get_registry():
    from modules.soul_registry import SoulRegistry
    from modules import config as cfg_module
    return SoulRegistry(cfg_module.CFG.souls_dir)


def _get_gap_resolver():
    from modules.soul_gap_resolver import SoulGapResolver
    from modules import config as cfg_module
    registry = _get_registry()
    return SoulGapResolver(cfg_module.CFG.souls_dir, registry)


def _get_evolver():
    from modules.soul_evolver import SoulEvolver
    from modules import config as cfg_module
    registry = _get_registry()
    return SoulEvolver(cfg_module.CFG.souls_dir, registry)


def _get_result_assembler():
    from modules.result_assembler import ResultAssembler
    return ResultAssembler(factory=_get_factory())


# ── Точки входа воркеров (нужны для multiprocessing) ─────────────────────

def _run_senior():
    from modules.workers import SeniorWorker
    SeniorWorker("senior_main", _get_factory(), _get_registry(), _get_gap_resolver()).run()

def _run_middle(direction: str):
    from modules.workers import MiddleWorker
    MiddleWorker(f"middle_{direction}", _get_factory(), direction).run()

def _run_judge():
    from modules.workers import JudgeWorker
    JudgeWorker("judge_main", _get_factory(), _get_evolver(), _get_result_assembler()).run()

def _run_watchdog():
    from modules.workers import WatchdogWorker
    WatchdogWorker("watchdog", _get_factory()).run()

def _run_notifier():
    from modules.notifier import NotifierWorker
    NotifierWorker("notifier").run()

def _run_agent(agent_id: str):
    from modules.workers import AgentWorker
    AgentWorker(agent_id, _get_factory()).run()


# ── Сохранение PID-ов ─────────────────────────────────────────────────────

def _save_pids(pids: list[int]):
    PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    PID_FILE.write_text(json.dumps(pids))

def _load_pids() -> list[int]:
    if not PID_FILE.exists():
        return []
    return json.loads(PID_FILE.read_text())


# ── Команды ───────────────────────────────────────────────────────────────

def cmd_bootstrap(args):
    """Инициализация Redis-схемы и создание базовых агентов."""
    from modules.redis_bus import get_redis, ping
    from modules.config import CFG

    print(f"Подключение к Redis: {CFG.redis_url}")
    if not ping():
        print("ОШИБКА: Redis недоступен.")
        print("Запусти:  docker run -d -p 6379:6379 redis:7-alpine")
        sys.exit(1)
    print("Redis: OK")

    # Запись runtime-конфига
    r = get_redis()
    r.hset("orchestra:config", mapping={
        "SLOW_THRESHOLD":      str(CFG.slow_threshold),
        "IDLE_TTL":            str(CFG.idle_ttl),
        "MAX_CLONES":          str(CFG.max_clones),
        "WATCHDOG_INTERVAL":   str(CFG.watchdog_interval),
        "SKILL_EVOLVE_EVERY":  str(CFG.skill_evolve_every),
    })

    factory = _get_factory()

    # Создать Senior (immortal singleton)
    print("\nСоздание Senior-оркестратора…")
    senior = factory.create_senior()
    print(f"  {senior.id}  [immortal={senior.immortal}]")

    # Создать Judge
    print("Создание Judge-оркестратора…")
    judge = factory.create_judge()
    print(f"  {judge.id}")

    # Сканируем направления из SOULs/agents/ — НЕ создаём AgentState'ы.
    # Агенты появятся лениво, когда Junior получит первую задачу для direction.
    from modules.soul_loader import SoulLoader
    loader = SoulLoader(CFG.souls_dir)
    directions = loader.available_agent_directions()
    print(f"\nДоступные направления (lazy spawn): {', '.join(directions)}")

    # Построить индекс душ
    print("\nПостроение индекса душ (SoulRegistry)…")
    from modules.soul_registry import SoulRegistry
    registry = SoulRegistry(CFG.souls_dir)
    count = registry.rebuild(use_llm=False)
    print(f"  Проиндексировано: {count} душ")

    print("\nBootstrap завершён.")
    print("\nЗапусти оркестр:  python orchestra_ctl.py start")


def cmd_start(args):
    """Запуск всех воркеров."""
    from modules.redis_bus import ping, AgentRegistry
    from modules.models import AgentRole

    if not ping():
        print("Redis недоступен. Запусти bootstrap сначала.")
        sys.exit(1)

    procs: list[multiprocessing.Process] = []
    pids: list[int] = []

    def spawn(target, *targs, name="worker"):
        p = multiprocessing.Process(target=target, args=targs, name=name, daemon=False)
        p.start()
        procs.append(p)
        pids.append(p.pid)
        print(f"  Запущен {name:45s} pid={p.pid}")

    print("Запуск оркестра (Senior+Judge+Watchdog+Notifier; Middle/Junior/Agent — lazy)…\n")

    # Постоянные воркеры
    spawn(_run_senior,   name="Senior")
    spawn(_run_judge,    name="Judge")
    spawn(_run_watchdog, name="Watchdog")
    spawn(_run_notifier, name="Notifier")

    _save_pids(pids)
    print(f"\nОркестр запущен ({len(procs)} процессов). PIDs: {PID_FILE}")
    print(f"Логи subprocess-воркеров: ~/.hermes/orchestra-workers.log")
    print("Мониторинг: python orchestra_ctl.py watch")
    print("Задача:     python orchestra_ctl.py submit \"<задача>\"")
    print("Остановка:  python orchestra_ctl.py stop")

    try:
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        print("\nОстановка…")
        for p in procs:
            p.terminate()


def cmd_stop(args):
    """Graceful shutdown всех процессов оркестра.

    Глушит два набора:
      1) Стартовые воркеры из PID_FILE (Senior, Judge, Watchdog, начальные AgentWorker)
      2) Динамически порождённые воркеры из Redis-сета (DYNAMIC_PIDS_KEY) —
         Junior, клоны, AgentWorker созданные на лету
    """
    import signal as _sig
    pids = set(_load_pids())

    # Динамические PID-ы из Redis (TODO 3)
    try:
        from modules.redis_bus import get_redis
        from modules.workers import DYNAMIC_PIDS_KEY
        dyn = {int(p) for p in get_redis().smembers(DYNAMIC_PIDS_KEY) if p.isdigit()}
        pids |= dyn
    except Exception as exc:
        print(f"(не удалось прочитать динамические PID: {exc})")

    if not pids:
        print("PIDs не найдены.")
        return

    stopped = 0
    for pid in pids:
        try:
            os.kill(pid, _sig.SIGTERM)
            stopped += 1
        except ProcessLookupError:
            pass
    print(f"SIGTERM отправлен {stopped}/{len(pids)} процессам.")

    PID_FILE.unlink(missing_ok=True)
    try:
        from modules.redis_bus import get_redis
        from modules.workers import DYNAMIC_PIDS_KEY
        r = get_redis()
        r.delete(DYNAMIC_PIDS_KEY)
        r.delete("orchestra:dir_refcount")
        # Чистим lifecycle/worker-ключи чтобы следующий start с нуля
        for pat in ("worker:running:*", "worker:pid:*", "dir_acquired:*"):
            for key in r.scan_iter(match=pat, count=200):
                r.delete(key)
    except Exception:
        pass


def cmd_status(args):
    """Статус Redis и агентов."""
    from modules.redis_bus import get_redis, ping
    from modules.redis_bus import AgentRegistry

    if ping():
        info = get_redis().info("server")
        print(f"Redis: OK  (v{info.get('redis_version','?')}, "
              f"uptime={info.get('uptime_in_seconds','?')}s)")
    else:
        print("Redis: НЕДОСТУПЕН")
        return

    agents = AgentRegistry.all_agents()
    print(f"\nАгентов зарегистрировано: {len(agents)}")
    by_role: dict = {}
    for a in agents:
        by_role[a.role.value] = by_role.get(a.role.value, 0) + 1
    for role, cnt in sorted(by_role.items()):
        print(f"  {role:22s}: {cnt}")

    pids = _load_pids()
    alive = sum(1 for p in pids if _pid_alive(p))
    print(f"\nВоркер-процессы: {alive}/{len(pids)} активных")


def cmd_submit(args):
    """Отправить задачу Senior-у. Опционально подписать на push."""
    from modules.redis_bus import publish_task, STREAM_SENIOR
    from modules.models import Task

    task_id = f"task_{uuid.uuid4().hex[:10]}"
    task = Task(
        id=task_id, content=args.task,
        direction="senior", source_agent_id="user",
    )
    publish_task(STREAM_SENIOR, task)

    if getattr(args, "notify", None):
        from modules.notifier import register_notification
        register_notification(task_id, args.notify)
        print(f"Push-уведомление зарегистрировано: {args.notify}")

    print(f"Задача принята: {task_id}")
    print(f"Мониторинг: python orchestra_ctl.py watch --task {task_id}")
    print(f"Результат:  python orchestra_ctl.py result {task_id}")


def cmd_result(args):
    """Получить результат задачи."""
    from modules.redis_bus import load_result
    data = load_result(args.task_id)
    if not data:
        print(f"Результат не найден для {args.task_id}. Задача ещё выполняется?")
        return

    print(f"Задача:  {args.task_id}")
    print(f"Статус:  {data.get('status','?')}")
    if data.get("score"):
        print(f"Score:   {data['score']}  (попыток: {data.get('attempts','?')})")

    if "final_result" in data:
        print("\n" + "="*60)
        print(data["final_result"])
    elif "decomposition" in data:
        from modules.redis_bus import get_redis
        r = get_redis()
        decomp = json.loads(data.get("decomposition", "{}"))
        print("\nДекомпозиция:")
        for direction, ids in decomp.items():
            for tid in ids:
                sub = r.hgetall(f"results:{tid}")
                st  = sub.get("status", "pending")
                res = sub.get("result", "")
                print(f"\n[{direction}] {tid} — {st}")
                if res:
                    print(f"  {res[:200]}{'…' if len(res)>200 else ''}")
    else:
        for k, v in data.items():
            print(f"  {k}: {v[:100]}{'…' if len(v)>100 else ''}")


def cmd_watch(args):
    """Стрим активности по всем направлениям + статусы результатов.

    Показывает:
      [SENIOR]   входящие задачи пользователя
      [MID/dir]  декомпозиция Senior → Middle
      [JUN/dir]  Middle → Junior
      [AGT/id]   Junior → Agent (с retry/iter если есть)
      [JUDGE>]   агент → Judge
      [DLQ]      DLQ-задачи
      [REZ]      обновления results:{tid}.status / score / attempts
      [W:dir]    жив ли worker-lock для middle/junior/agent
    """
    from modules.redis_bus import get_redis, STREAM_SENIOR, STREAM_JUDGE_IN
    from modules.task_resilience import DLQ_STREAM

    r = get_redis()
    # Стартуем со значения "$" — только новые сообщения после запуска watch
    streams: dict[str, str] = {
        STREAM_SENIOR:    "$",
        STREAM_JUDGE_IN:  "$",
        DLQ_STREAM:       "$",
    }
    # Динамически добавляем tasks:middle:*, tasks:junior:*, tasks:agent:*
    for pat in ("tasks:middle:*", "tasks:junior:*", "tasks:agent:*"):
        for k in r.scan_iter(match=pat, count=200):
            streams[k] = "$"

    # Снапшот results:* (status/score/attempts) — детектируем изменения
    results_state: dict[str, dict] = {}
    for k in r.scan_iter(match="results:*", count=200):
        if k.startswith("results:assemble_lock"):
            continue
        results_state[k] = r.hgetall(k)

    def short(s: str, n: int = 70) -> str:
        s = (s or "").replace("\n", " ").strip()
        return s if len(s) <= n else s[:n] + "…"

    def label(stream: str) -> str:
        if stream == STREAM_SENIOR:       return "SENIOR"
        if stream == STREAM_JUDGE_IN:     return "JUDGE>"
        if stream == DLQ_STREAM:          return "DLQ"
        if stream.startswith("tasks:middle:"):
            return f"MID/{stream.split(':')[-1]}"
        if stream.startswith("tasks:junior:"):
            return f"JUN/{stream.split(':')[-1]}"
        if stream.startswith("tasks:agent:"):
            return f"AGT/{stream.split(':')[-1][:18]}"
        return stream

    def print_workers_snapshot():
        """Раз в N тиков показывает кто из worker-lock'ов жив."""
        alive: dict[str, list[str]] = {}
        for k in r.scan_iter(match="worker:running:*", count=200):
            parts = k.split(":", 3)
            if len(parts) < 4:
                continue
            kind, direction = parts[2], parts[3]
            alive.setdefault(kind, []).append(direction)
        if alive:
            line = "  ".join(
                f"{k}={','.join(sorted(v))}" for k, v in sorted(alive.items())
            )
            print(f"  [W:alive] {line}")

    print("Мониторинг" + (f" (фильтр: {args.task})" if args.task else "")
          + "…  Ctrl+C для выхода\n")
    print_workers_snapshot()

    tick = 0
    try:
        while True:
            tick += 1
            # Раз в ~10 секунд — пересобрать список stream'ов и снапшот workers
            if tick % 5 == 0:
                for pat in ("tasks:middle:*", "tasks:junior:*", "tasks:agent:*"):
                    for k in r.scan_iter(match=pat, count=200):
                        streams.setdefault(k, "$")
                print_workers_snapshot()

            # Чтение всех streams
            raw = r.xread(streams, count=20, block=2000)
            if raw:
                for stream, msgs in raw:
                    for eid, fields in msgs:
                        streams[stream] = eid
                        try:
                            t = json.loads(fields.get("task", "{}"))
                            tid = t.get("id", "?")
                            if args.task and args.task not in tid:
                                continue
                            extra = []
                            if t.get("judge_iteration", 0):
                                extra.append(f"iter={t['judge_iteration']}")
                            if t.get("retry_count", 0):
                                extra.append(f"retry={t['retry_count']}")
                            if t.get("parent_task_id"):
                                extra.append(f"par={t['parent_task_id'][:14]}")
                            ex = (" " + " ".join(extra)) if extra else ""
                            preview = short(t.get("content", "") or t.get("result", ""))
                            print(f"  [{label(stream):>10s}] {tid}{ex} | {preview}")
                        except Exception:
                            print(f"  [{label(stream):>10s}] {fields}")

            # Дифф по results:* — статус/score/attempts/final_result
            for k in r.scan_iter(match="results:*", count=200):
                if k.startswith("results:assemble_lock"):
                    continue
                tid = k.split(":", 1)[1]
                if args.task and args.task not in tid:
                    continue
                cur = r.hgetall(k)
                prev = results_state.get(k, {})
                changed = {f: cur[f] for f in cur if cur[f] != prev.get(f)}
                results_state[k] = cur
                if not changed:
                    continue
                interesting = ("status", "score", "attempts", "final_result",
                               "gap_count", "last_error")
                pieces = []
                for f in interesting:
                    if f in changed:
                        v = changed[f]
                        if f == "final_result":
                            v = f"<len={len(v)}>"
                        pieces.append(f"{f}={short(str(v), 50)}")
                if pieces:
                    print(f"  [{'REZ':>10s}] {tid} | {' '.join(pieces)}")
    except KeyboardInterrupt:
        print("\nОстановлено.")


def cmd_agents(args):
    """Список всех агентов."""
    from modules.redis_bus import AgentRegistry
    agents = AgentRegistry.all_agents()
    if not agents:
        print("Агентов нет. Запусти bootstrap.")
        return
    print(f"{'ID':38s} {'РОЛЬ':20s} {'НАПРАВЛЕНИЕ':14s} {'ЗАДАЧ':6s} {'ms':7s} {'КЛОНОВ'}")
    print("-"*95)
    for a in sorted(agents, key=lambda x: (x.direction, x.id)):
        clones = f"{len(a.clone_ids)}" if a.clone_ids else "-"
        immortal = " [immortal]" if a.immortal else ""
        print(f"{a.id:38s} {a.role.value:20s} {a.direction:14s} "
              f"{a.tasks_completed:6d} {a.avg_latency_ms:7.0f} {clones}{immortal}")


def cmd_metrics(args):
    """Метрики латентности по направлениям."""
    from modules.redis_bus import get_redis
    r = get_redis()
    directions = set(v for v in r.hvals("orchestra:agents")
                     if v not in ("senior","middle","junior","judge"))
    if not directions:
        print("Метрик нет.")
        return
    from modules import config as cfg_module
    for d in sorted(directions):
        members = r.zrange(f"metrics:latency:{d}", 0, -1, withscores=True)
        if not members:
            continue
        print(f"\n{d.upper()}")
        for aid, score in members:
            print(f"  {aid:38s} {score:7.0f} ms")
        scores = [s for _, s in members if s > 0]
        if scores:
            med = statistics.median(scores) if len(scores) > 1 else scores[0]
            print(f"  медиана={med:.0f}ms  порог клонирования={med*cfg_module.CFG.slow_threshold:.0f}ms")


def cmd_agent_info(args):
    """Душа и скилл агента."""
    from modules.redis_bus import AgentRegistry
    agent = AgentRegistry.load(args.agent_id)
    if not agent:
        print(f"Агент {args.agent_id} не найден.")
        return
    print(f"ID:         {agent.id}")
    print(f"Роль:       {agent.role.value}{'  [immortal]' if agent.immortal else ''}")
    print(f"Направление:{agent.direction}")
    print(f"\nДУША")
    print(f"  {agent.soul.personality[:200]}…")
    print(f"  Ценности: {', '.join(agent.soul.values)}")
    print(f"  Doubt:    {agent.soul.doubt_level}")
    print(f"\nСКИЛЛ v{agent.skill.version}")
    print(f"  {agent.skill.description}")
    if agent.skill.strengths:
        print(f"  Сильные стороны: {', '.join(agent.skill.strengths)}")
    if agent.skill.learned_patterns:
        print(f"  Паттерны:")
        for p in agent.skill.learned_patterns[-5:]:
            print(f"    — {p}")
    print(f"\nМЕТРИКИ")
    print(f"  Задач выполнено: {agent.tasks_completed}")
    print(f"  Ср. латентность: {agent.avg_latency_ms:.0f}ms")
    if agent.clone_ids:
        print(f"  Клоны: {', '.join(agent.clone_ids)}")


def cmd_add_agent(args):
    """Добавить нового агента динамически."""
    import yaml
    from modules import config as cfg_module

    # Создаём soul.yaml в SOULs/agents/<direction>/
    soul_dir = cfg_module.CFG.souls_dir / "agents" / args.direction
    soul_dir.mkdir(parents=True, exist_ok=True)
    soul_path = soul_dir / "soul.yaml"

    if soul_path.exists() and not args.force:
        print(f"Файл уже существует: {soul_path}")
        print("Используй --force для перезаписи.")
        return

    soul_data = {
        "direction": args.direction,
        "soul": {
            "personality": args.soul,
            "values": [v.strip() for v in args.values.split(",")] if args.values else [],
            "doubt_level": 0.0,
        },
        "skill": {
            "version": 1,
            "description": args.skill,
            "strengths": [],
            "learned_patterns": [],
        },
    }
    soul_path.write_text(
        f"# SOULs/agents/{args.direction}/soul.yaml\n"
        f"# Создан через orchestra_ctl.py add-agent\n\n"
        + yaml.dump(soul_data, allow_unicode=True, default_flow_style=False)
    )
    print(f"Soul файл создан: {soul_path}")

    # Создаём агента в Redis
    factory = _get_factory()
    agent = factory.create_agent(args.direction)
    print(f"Агент создан: {agent.id}")
    print(f"\nПерезапусти оркестр для запуска воркера нового агента.")


def cmd_reset_skill(args):
    """Сбросить скилл агента до исходного из soul.yaml."""
    from modules.redis_bus import AgentRegistry
    from modules import config as cfg_module
    from modules.soul_loader import SoulLoader

    agent = AgentRegistry.load(args.agent_id)
    if not agent:
        print(f"Агент {args.agent_id} не найден.")
        return

    loader = SoulLoader(cfg_module.CFG.souls_dir)
    try:
        _, skill = loader.load_agent(agent.direction)
        agent.skill = skill
        AgentRegistry.save(agent)
        AgentRegistry.reset_skill_history(agent.id)
        print(f"Скилл {args.agent_id} сброшен до v1.")
    except FileNotFoundError as e:
        print(f"Ошибка: {e}")


def cmd_gc(args):
    """Принудительная очистка idle-клонов."""
    from modules.workers import WatchdogWorker
    from modules.redis_bus import get_redis

    w = WatchdogWorker("ctl_gc", _get_factory())
    r = get_redis()
    directions = set(v for v in r.hvals("orchestra:agents")
                     if v not in ("senior","middle","junior","judge"))
    for d in directions:
        w._gc_idle_clones(d)
    print("GC завершён.")


def cmd_flush(args):
    """ОПАСНО: удалить все ключи оркестра."""
    confirm = input("Введи 'yes' для удаления всех ключей оркестра: ")
    if confirm.strip().lower() != "yes":
        print("Отменено.")
        return
    from modules.redis_bus import get_redis
    r = get_redis()
    patterns = ["tasks:*","agent:*","orchestra:*","metrics:*","judge:*","results:*"]
    deleted = 0
    for p in patterns:
        keys = r.keys(p)
        if keys:
            deleted += r.delete(*keys)
    print(f"Удалено {deleted} ключей.")


def cmd_gap_resolve(args):
    """Разрешить запросы на недостающие души."""
    from modules.soul_registry import SoulRegistry
    from modules.soul_gap_resolver import SoulGapResolver
    from modules.models import SoulGapAction
    from modules import config as cfg_module

    registry = SoulRegistry(cfg_module.CFG.souls_dir)
    resolver = SoulGapResolver(cfg_module.CFG.souls_dir, registry)
    pending  = resolver.get_pending_requests()

    if not pending:
        print("Нет pending запросов на новые души.")
        return

    print(f"Pending запросов: {len(pending)}")
    for req in pending:
        print(f"  [{req.request_id}] direction={req.direction}  "
              f"задача={req.task_for_agent[:60]}…")

    if args.action == "upload":
        print("\n" + resolver.get_upload_instructions())
        if args.files:
            resolved = resolver.resolve(SoulGapAction.UPLOAD, uploaded_files=args.files)
            print(f"\nЗагружено: {', '.join(resolved) or 'ничего'}")
    elif args.action == "create":
        print("\nAI создаёт души…")
        resolved = resolver.resolve(SoulGapAction.CREATE)
        if resolved:
            print(f"\nСозданы направления: {', '.join(resolved)}")
            registry.rebuild(use_llm=False)
            print("Индекс обновлён.")
        else:
            print("Не удалось создать ни одной души.")
    elif args.action == "skip":
        resolved = resolver.resolve(SoulGapAction.SKIP)
        print(f"Пропущено: {len(resolved)} запросов.")


def cmd_dlq_list(args):
    """Показать задачи в Dead Letter Queue."""
    from modules.task_resilience import get_dlq_tasks
    tasks = get_dlq_tasks(count=args.count)
    if not tasks:
        print("DLQ пуст.")
        return
    print(f"{'ID':25s} {'DIRECTION':12s} {'RETRY':5s} ОШИБКА")
    print("-"*80)
    for t in tasks:
        err = str(t.get("last_error", ""))[:40]
        print(f"{t.get('id','?'):25s} {t.get('direction','?'):12s} "
              f"{t.get('retry_count',0):5d} {err}")


def cmd_dlq_retry(args):
    """Повторить задачу из DLQ."""
    from modules.task_resilience import retry_dlq_task
    ok = retry_dlq_task(args.task_id)
    if ok:
        print(f"Задача {args.task_id} переотправлена.")
    else:
        print(f"Не удалось переотправить {args.task_id}.")


def cmd_evolution_log(args):
    """История эволюции души агента."""
    from modules.soul_evolver import SoulEvolver
    from modules.soul_registry import SoulRegistry
    from modules import config as cfg_module
    registry = SoulRegistry(cfg_module.CFG.souls_dir)
    evolver  = SoulEvolver(cfg_module.CFG.souls_dir, registry)
    entries  = evolver.get_evolution_log(args.direction, last_n=args.n)
    if not entries:
        print(f"История эволюции для '{args.direction}' пуста.")
        return
    print(f"Последние {len(entries)} изменений '{args.direction}':\n")
    for e in entries:
        ts  = time.strftime("%Y-%m-%d %H:%M", time.localtime(e.get("ts", 0)))
        imp = e.get("improvements", {})
        changes = []
        if imp.get("new_strength"):     changes.append(f"сила: {imp['new_strength']}")
        if imp.get("new_pattern"):      changes.append(f"паттерн: {imp['new_pattern'][:40]}")
        if imp.get("skill_refinement"): changes.append("скилл уточнён")
        if imp.get("personality_note"): changes.append("личность уточнена")
        print(f"  {ts}  {', '.join(changes) or '(без изменений)'}")
        if e.get("task_preview"):
            print(f"           задача: {e['task_preview'][:80]}")


def cmd_ask(args):
    """End-to-end: submit задачу, стримит прогресс, печатает финал.

    Удобно для бота — одна команда от запроса до ответа.
    Форматы вывода:
      --format text  : человекочитаемо (default)
      --format json  : JSON-lines, каждый event на своей строке
                       (для парсинга из TG бота / скрипта)
    """
    from modules.redis_bus import publish_task, get_redis, load_result, STREAM_SENIOR
    from modules.models import Task

    task_id = f"task_{uuid.uuid4().hex[:10]}"
    task = Task(
        id=task_id, content=args.task,
        direction="senior", source_agent_id="user",
    )
    publish_task(STREAM_SENIOR, task)

    def emit(event: str, **kwargs):
        if args.format == "json":
            print(json.dumps({"event": event, "task_id": task_id, **kwargs},
                             ensure_ascii=False), flush=True)
        else:
            if event == "submitted":
                print(f"📨 Submitted {task_id}", flush=True)
            elif event == "progress":
                print(f"  · {kwargs.get('msg','')}", flush=True)
            elif event == "subtask_done":
                sid = kwargs.get('subtask_id','')
                print(f"  ✓ {sid}  score={kwargs.get('score','?')}  "
                      f"attempts={kwargs.get('attempts','?')}", flush=True)
            elif event == "subtask_rework":
                print(f"  ↻ {kwargs.get('subtask_id','')} rework "
                      f"score={kwargs.get('score','?')}", flush=True)
            elif event == "gap":
                print(f"⚠ GAP REQUEST: {kwargs.get('msg','')}", flush=True)
            elif event == "timeout":
                print(f"⌛ Timeout после {kwargs.get('seconds','?')}s", flush=True)
            elif event == "final":
                print(f"\n{'='*60}\n{kwargs.get('result','')}\n{'='*60}", flush=True)
            elif event == "failed":
                print(f"\n✗ FAILED: {kwargs.get('reason','')}", flush=True)

    emit("submitted")

    r = get_redis()
    seen_subtask_status: dict[str, str] = {}
    last_root_status = ""
    start_ts = time.time()
    deadline = start_ts + args.timeout

    while time.time() < deadline:
        # Корневая задача
        root = load_result(task_id)
        root_status = root.get("status", "")
        if root_status != last_root_status:
            emit("progress", msg=f"status={root_status}")
            last_root_status = root_status

        # Gap-уведомление
        if root.get("gap_notice") and not seen_subtask_status.get("__gap__"):
            seen_subtask_status["__gap__"] = "shown"
            emit("gap", msg=f"требуется {root.get('gap_count','?')} новых душ. "
                            f"Запусти `orchestra_ctl gap-resolve --action create`")

        # Дифф по сабтаскам
        decomp_raw = root.get("decomposition")
        if decomp_raw:
            try:
                decomp = json.loads(decomp_raw)
            except Exception:
                decomp = {}
            for direction, ids in decomp.items():
                for sid in ids:
                    sub = r.hgetall(f"results:{sid}")
                    st  = sub.get("status", "")
                    if not st:
                        continue
                    prev = seen_subtask_status.get(sid, "")
                    if st != prev:
                        if st == "done":
                            emit("subtask_done", subtask_id=sid,
                                 direction=direction,
                                 score=sub.get("score","?"),
                                 attempts=sub.get("attempts","?"))
                        elif st == "dlq":
                            emit("subtask_dlq", subtask_id=sid,
                                 error=sub.get("last_error",""))
                        seen_subtask_status[sid] = st

        # Финал
        if root.get("final_result"):
            emit("final", result=root["final_result"],
                 elapsed=round(time.time() - start_ts, 1),
                 score=root.get("score",""),
                 status=root_status)
            return

        if root_status in ("failed",):
            emit("failed", reason=root.get("last_error",""))
            return

        time.sleep(args.poll)

    emit("timeout", seconds=args.timeout)


def cmd_wait(args):
    """Подождать готовности задачи и напечатать финальный результат."""
    from modules.redis_bus import load_result
    deadline = time.time() + args.timeout
    last = ""
    while time.time() < deadline:
        data = load_result(args.task_id)
        st = data.get("status", "")
        if st != last:
            print(f"  status={st}", flush=True)
            last = st
        if data.get("final_result"):
            print("\n" + "="*60)
            print(data["final_result"])
            return
        if st == "failed":
            print(f"\nFAILED: {data.get('last_error','')}")
            return
        time.sleep(args.poll)
    print(f"\nTimeout {args.timeout}s, последний статус: {last}")


def cmd_tasks_active(args):
    """Список активных (незавершённых) задач.

    Сканирует results:* и показывает те где status НЕ в терминальных.
    Поддерживает --json для парсинга ботом.
    """
    from modules.redis_bus import get_redis
    r = get_redis()
    TERMINAL = {"done", "dlq", "failed", "partial_dlq"}

    rows = []
    for key in r.scan_iter(match="results:*", count=200):
        if ":" not in key or key.startswith("results:assemble_lock"):
            continue
        tid = key.split(":", 1)[1]
        d = r.hgetall(key)
        status = d.get("status", "")
        # Корневые задачи начинаются с "task_" и не имеют "_" внутри после
        # префикса (сабтаски имеют вид task_XXX_direction_YYY)
        if not tid.startswith("task_") or tid.count("_") > 1:
            continue
        if status in TERMINAL:
            continue
        rows.append({
            "task_id":  tid,
            "status":   status or "pending",
            "original": d.get("original_task", "")[:80],
            "created_at": d.get("created_at", ""),
            "gap_count": d.get("gap_count", "0"),
        })

    if getattr(args, "json", False):
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return

    if not rows:
        print("Активных задач нет.")
        return
    print(f"{'TASK_ID':24s} {'СТАТУС':14s} {'GAP':4s} ЗАПРОС")
    print("-" * 100)
    for row in sorted(rows, key=lambda x: x["task_id"]):
        print(f"{row['task_id']:24s} {row['status']:14s} "
              f"{row['gap_count']:>4s} {row['original']}")


def cmd_tasks_by_date(args):
    """Все корневые задачи за конкретную дату (YYYY-MM-DD или 'today')."""
    from datetime import datetime, timedelta
    from modules.redis_bus import get_redis
    r = get_redis()

    date_str = args.date.lower()
    if date_str in ("today", "сегодня"):
        date_str = datetime.now().strftime("%Y-%m-%d")
    elif date_str in ("yesterday", "вчера"):
        date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    try:
        day_start = datetime.strptime(date_str, "%Y-%m-%d").timestamp()
        day_end = day_start + 86400
    except ValueError:
        print(f"Неверный формат даты: {args.date} (ожидается YYYY-MM-DD)")
        return

    rows = []
    for key in r.scan_iter(match="results:*", count=200):
        if ":" not in key or key.startswith("results:assemble_lock"):
            continue
        tid = key.split(":", 1)[1]
        if not tid.startswith("task_") or tid.count("_") > 1:
            continue
        d = r.hgetall(key)
        # created_at не всегда есть; альтернативно смотрим на assembled_at / любые ts
        ts_str = d.get("created_at") or d.get("assembled_at", "")
        if not ts_str:
            continue
        try:
            ts = float(ts_str)
        except ValueError:
            continue
        if not (day_start <= ts < day_end):
            continue
        rows.append({
            "task_id":  tid,
            "status":   d.get("status", "?"),
            "score":    d.get("score", ""),
            "attempts": d.get("attempts", ""),
            "original": d.get("original_task", "")[:80],
            "ts":       ts,
        })

    if getattr(args, "json", False):
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return

    if not rows:
        print(f"За {date_str} задач нет.")
        return
    print(f"Задачи за {date_str}:\n")
    print(f"{'ВРЕМЯ':9s} {'TASK_ID':24s} {'СТАТУС':14s} {'SCORE':6s} ЗАПРОС")
    print("-" * 110)
    for r_ in sorted(rows, key=lambda x: x["ts"]):
        t = time.strftime("%H:%M:%S", time.localtime(r_["ts"]))
        print(f"{t:9s} {r_['task_id']:24s} {r_['status']:14s} "
              f"{(r_['score'] or '-'):6s} {r_['original']}")


def cmd_diag(args):
    """Полная диагностика одной задачи: статус, decomp, notify, history."""
    from modules.redis_bus import get_redis
    r = get_redis()
    tid = args.task_id

    print(f"=== Диагностика {tid} ===\n")

    # 1. Корневой results
    data = r.hgetall(f"results:{tid}")
    if not data:
        print(f"✗ Ключ results:{tid} НЕ найден.")
        print("  Возможно задача никогда не доходила до Senior — Hermes-агент")
        print("  не вызвал orchestra_submit, либо передал другой task_id.")
        return

    print("results:%s:" % tid)
    for k in ("status", "score", "attempts", "gap_count", "assembled_at",
              "last_error", "original_task"):
        v = data.get(k, "")
        if v:
            print(f"  {k:18s} = {v[:120]}")
    if "final_result" in data:
        print(f"  final_result       = <len={len(data['final_result'])}> "
              f"{data['final_result'][:100]}…")
    else:
        print(f"  final_result       = (отсутствует)")

    # 2. Notify подписка
    print("\nNotification:")
    notify_target = r.get(f"notify:{tid}")
    sent_at = r.get(f"notify_sent:{tid}")
    if notify_target:
        print(f"  ✓ Подписка: target={notify_target}")
    else:
        print(f"  ✗ Подписки нет (notify:{tid} отсутствует) — push не уйдёт.")
        print(f"    Можно зарегистрировать сейчас:")
        print(f"      orchestra_ctl notify-now {tid} tg:<твой_chat_id>")
    if sent_at:
        print(f"  ✓ Уже отправлено: {sent_at}")

    # 3. Декомпозиция
    decomp_raw = data.get("decomposition")
    if decomp_raw:
        print("\nDecomposition:")
        try:
            decomp = json.loads(decomp_raw)
            for direction, ids in decomp.items():
                print(f"  {direction}:")
                for sid in ids:
                    sub = r.hgetall(f"results:{sid}")
                    st = sub.get("status", "(пусто)")
                    sc = sub.get("score", "")
                    at = sub.get("attempts", "")
                    err = sub.get("last_error", "")[:60]
                    line = f"    · {sid}  {st}"
                    if sc:  line += f"  score={sc}"
                    if at:  line += f"  attempts={at}"
                    if err: line += f"  err={err}"
                    print(line)
        except json.JSONDecodeError:
            print(f"  (невалидный JSON: {decomp_raw[:200]})")
    else:
        print("\nDecomposition: (отсутствует — Senior не декомпозировал)")

    # 4. Refcount/аренда направлений
    acquired = r.smembers(f"dir_acquired:{tid}")
    if acquired:
        print(f"\nDirections held by this task: {', '.join(sorted(acquired))}")
    else:
        print("\nDirections held: (нет — либо teardown прошёл, либо acquire не сработал)")

    # 5. Worker-процессы по direction'ам этой задачи
    if decomp_raw:
        try:
            decomp = json.loads(decomp_raw)
            print("\nLive workers per direction:")
            for direction in decomp:
                live = []
                for kind in ("middle", "junior", "agent"):
                    lock = r.get(f"worker:running:{kind}:{direction}")
                    pid = r.get(f"worker:pid:{kind}:{direction}")
                    if lock or pid:
                        is_alive = _pid_alive(int(pid)) if pid and pid.isdigit() else False
                        live.append(f"{kind}={'✓' if is_alive else '✗'}({pid or '-'})")
                print(f"  {direction:14s}  {' '.join(live) or '(нет)'}")
        except Exception:
            pass

    print()


def cmd_notify_now(args):
    """Зарегистрировать push-уведомление вручную для уже отправленной задачи.

    Полезно когда Hermes-агент не передал notify_target при submit,
    а тебе всё равно хочется получить результат в TG.
    """
    from modules.notifier import register_notification
    register_notification(args.task_id, args.target)
    print(f"Подписка для {args.task_id} → {args.target} создана.")
    print("NotifierWorker подхватит на следующем тике (≤ 3 сек).")


def cmd_lifecycle(args):
    """Что сейчас 'арендовано' и какие subprocess'ы живы."""
    from modules.redis_bus import get_redis
    r = get_redis()

    refs = r.hgetall("orchestra:dir_refcount")
    print("Refcount по направлениям:")
    if not refs:
        print("  (пусто — нет активных задач, удерживающих direction'ы)")
    else:
        for d, n in sorted(refs.items()):
            print(f"  {d:14s}  {n}")

    print("\nЖивые worker subprocess'ы:")
    pids = {}
    for key in r.scan_iter(match="worker:pid:*", count=200):
        parts = key.split(":")
        if len(parts) < 4:
            continue
        kind, direction = parts[2], parts[3]
        pid = r.get(key)
        try:
            pid_i = int(pid)
            alive = _pid_alive(pid_i)
        except (TypeError, ValueError):
            pid_i, alive = 0, False
        pids.setdefault(direction, []).append((kind, pid_i, alive))
    if not pids:
        print("  (пусто)")
    else:
        for d, items in sorted(pids.items()):
            for kind, pid, alive in sorted(items):
                marker = "✓" if alive else "✗"
                print(f"  {marker} {kind:7s} {d:14s} pid={pid}")

    print("\nАрендованные direction'ы по задачам:")
    found = False
    for key in r.scan_iter(match="dir_acquired:*", count=200):
        tid = key.split(":", 1)[1]
        dirs = r.smembers(key)
        if dirs:
            found = True
            print(f"  {tid}  →  {', '.join(sorted(dirs))}")
    if not found:
        print("  (нет активных аренд)")


def cmd_health(args):
    """Проверить что всё необходимое для работы оркестра доступно.

    Проверяет:
      - Redis ping
      - Hermes-proxy на CFG.openai_base_url (GET /models)
      - Наличие SOULs/ и индекса
    Возвращает exit code 0 если всё OK, иначе 1.
    """
    from modules.redis_bus import ping
    from modules import config as cfg_module
    import urllib.request, urllib.error

    ok = True

    print("Health check:\n")

    # 1. Redis
    if ping():
        print(f"  ✓ Redis           OK  ({cfg_module.CFG.redis_url})")
    else:
        print(f"  ✗ Redis           НЕТ ({cfg_module.CFG.redis_url})")
        print(f"     Запусти: docker run -d -p 6379:6379 redis:7-alpine")
        ok = False

    # 2. LLM endpoint (Hermes proxy или прямой xAI)
    base = cfg_module.CFG.openai_base_url.rstrip("/")
    probe_url = f"{base}/models"
    try:
        req = urllib.request.Request(
            probe_url,
            headers={"Authorization": f"Bearer {cfg_module.CFG.openai_api_key or 'x'}"},
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            if 200 <= resp.status < 300:
                print(f"  ✓ LLM endpoint    OK  ({base})")
            else:
                print(f"  ⚠ LLM endpoint    HTTP {resp.status} ({base})")
                ok = False
    except urllib.error.HTTPError as e:
        # 401/403 на /models — endpoint жив, просто требует auth. Это ок:
        # реальные запросы пойдут через openai SDK с правильным заголовком.
        if e.code in (401, 403):
            print(f"  ✓ LLM endpoint    жив (HTTP {e.code} на /models — auth, не сетевая)")
        else:
            print(f"  ✗ LLM endpoint    HTTP {e.code} ({base})")
            ok = False
    except Exception as e:
        print(f"  ✗ LLM endpoint    {type(e).__name__}: {e} ({base})")
        print(f"     Запусти: nohup hermes proxy start --provider xai > ~/.hermes/proxy.log 2>&1 &")
        ok = False

    # 3. SOULs / индекс
    souls_dir = cfg_module.CFG.souls_dir
    if souls_dir.exists():
        n_agents = sum(1 for d in (souls_dir / "agents").glob("*/soul.yaml"))
        print(f"  ✓ SOULs/          OK  ({n_agents} agent souls в {souls_dir})")
    else:
        print(f"  ✗ SOULs/          НЕТ ({souls_dir})")
        ok = False

    # 4. Worker процессы (если есть pidfile)
    pids = _load_pids()
    if pids:
        alive = sum(1 for p in pids if _pid_alive(p))
        sym = "✓" if alive == len(pids) else "⚠"
        print(f"  {sym} Workers          {alive}/{len(pids)} живы")
    else:
        print(f"  · Workers          не запущены (orchestra_ctl start)")

    print()
    if ok:
        print("Всё ОК. Можно стартовать.")
        return
    print("Обнаружены проблемы. Исправь и перезапусти.")
    sys.exit(1)


def cmd_config(args):
    """Показать ЭФФЕКТИВНУЮ конфигурацию (после env-override).

    Полезно когда что-то странное происходит: убедиться какой реально
    model/base_url/api_key подхватились в текущем shell.
    """
    from modules import config as cfg_module
    c = cfg_module.CFG

    def mask(s: str, keep: int = 4) -> str:
        if not s: return "<empty>"
        if len(s) <= keep * 2: return "<short>"
        return s[:keep] + "…" + s[-keep:]

    print("Эффективная конфигурация оркестра:\n")
    print(f"  redis_url            : {c.redis_url}")
    print(f"  model                : {c.model}")
    print(f"  openai_base_url      : {c.openai_base_url}")
    print(f"  openai_api_key       : {mask(c.openai_api_key)}")
    print(f"  anthropic_api_key    : {mask(c.anthropic_api_key)}")
    print(f"  embedding_model      : {c.embedding_model}")
    print(f"  use_embeddings       : {c.use_embeddings}")
    print(f"  judge_pass_threshold : {c.judge_pass_threshold}")
    print(f"  judge_max_iterations : {c.judge_max_iterations}")
    print(f"  slow_threshold       : {c.slow_threshold}")
    print(f"  watchdog_interval    : {c.watchdog_interval}")
    print(f"  souls_dir            : {c.souls_dir}")

    print("\nИсточник значений (env vs default):")
    keys = [
        ("REDIS_URL", c.redis_url),
        ("ORCHESTRA_MODEL", c.model),
        ("OPENAI_BASE_URL", c.openai_base_url),
        ("OPENAI_API_KEY", c.openai_api_key),
        ("ORCHESTRA_EMBEDDING_MODEL", c.embedding_model),
        ("ORCHESTRA_JUDGE_PASS_THRESHOLD", str(c.judge_pass_threshold)),
        ("ORCHESTRA_JUDGE_MAX_ITER", str(c.judge_max_iterations)),
    ]
    for env_name, effective in keys:
        env_val = os.environ.get(env_name)
        if env_val:
            src = f"env={env_val!r}"
        else:
            src = "default"
        print(f"  {env_name:32s} → {src}")


def cmd_judge_history(args):
    """Показать историю попыток Judge для конкретной (под)задачи."""
    from modules.redis_bus import get_redis
    r = get_redis()
    raw = r.lrange(f"judge:history:{args.task_id}", 0, -1)
    if not raw:
        print(f"История пуста для {args.task_id} (либо задача ещё не оценивалась, "
              "либо TTL истёк).")
        return
    print(f"История Judge для {args.task_id}:\n")
    for i, line in enumerate(raw):
        try:
            e = json.loads(line)
        except Exception:
            continue
        ts = time.strftime("%H:%M:%S", time.localtime(e.get("ts", 0)))
        print(f"  попытка {e.get('attempt','?'):>2}  "
              f"score={e.get('score',0):.2f}  "
              f"agent={e.get('agent_id','?')[:30]}  ts={ts}")
        if e.get("verdict"):
            print(f"    verdict:  {e['verdict'][:140]}")
        if e.get("critique"):
            print(f"    critique: {e['critique'][:140]}")
        if e.get("result"):
            print(f"    result:   {e['result'][:120]}{'…' if len(e['result'])>120 else ''}")
        print()


def cmd_soul_index(args):
    """Показать / перестроить динамический каталог душ."""
    from modules.soul_registry import SoulRegistry
    from modules import config as cfg_module
    registry = SoulRegistry(cfg_module.CFG.souls_dir)
    if args.rebuild:
        count = registry.rebuild(use_llm=args.llm)
        print(f"Индекс перестроен: {count} записей.")
        return
    entries = registry.get_all(role_type=args.type or None)
    if not entries:
        print("Индекс пуст. Запусти: python orchestra_ctl.py soul-index --rebuild")
        return
    print(f"{'DIRECTION':16s} {'ТИП':14s} {'ТЕГИ':35s} ПРЕВЬЮ")
    print("-"*100)
    for e in sorted(entries, key=lambda x: (x.role_type, x.direction)):
        tags    = ", ".join(e.tags[:5])
        preview = e.skill_preview[:40]
        print(f"{e.direction:16s} {e.role_type:14s} {tags:35s} {preview}")


def cmd_souls(args):
    """Показать все доступные души из SOULs/."""
    from modules import config as cfg_module
    from modules.soul_loader import SoulLoader

    loader = SoulLoader(cfg_module.CFG.souls_dir)
    print(f"SOULs директория: {cfg_module.CFG.souls_dir}\n")

    print("ОРКЕСТРАТОРЫ:")
    for role in ("senior", "middle", "junior", "judge"):
        path = cfg_module.CFG.souls_dir / "orchestrators" / role / "soul.yaml"
        exists = "OK" if path.exists() else "ОТСУТСТВУЕТ"
        meta = loader.get_orchestrator_meta(role) if path.exists() else {}
        flags = "  ".join(k for k, v in meta.items() if v)
        print(f"  {role:10s} [{exists}]  {flags}")

    print("\nАГЕНТЫ:")
    for direction in sorted(loader.available_agent_directions()):
        path = cfg_module.CFG.souls_dir / "agents" / direction / "soul.yaml"
        print(f"  {direction:14s}  {path}")


# ── Утилиты ───────────────────────────────────────────────────────────────

def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


# ── statistics нужен для cmd_metrics ─────────────────────────────────────
import statistics


# ── Main ──────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(prog="orchestra_ctl", description="Управление оркестром агентов")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("bootstrap",   help="Инициализация Redis + создание базовых агентов")
    sub.add_parser("start",       help="Запуск всех воркеров")
    sub.add_parser("stop",        help="Остановка всех воркеров")
    sub.add_parser("status",      help="Статус системы")

    ps = sub.add_parser("submit", help="Отправить задачу")
    ps.add_argument("task")
    ps.add_argument("--notify", default="",
                    help="Push при готовности. Форматы: tg:<chat_id>, "
                         "slack:<channel>, whatsapp:<phone>, webhook:<url>, log:")

    pr = sub.add_parser("result", help="Получить результат")
    pr.add_argument("task_id")

    pw = sub.add_parser("watch",  help="Стрим активности")
    pw.add_argument("--task", default="")

    sub.add_parser("agents",      help="Список агентов")
    sub.add_parser("metrics",     help="Метрики латентности")

    pi = sub.add_parser("agent-info", help="Душа и скилл агента")
    pi.add_argument("agent_id")

    pa = sub.add_parser("add-agent", help="Добавить нового агента")
    pa.add_argument("--direction", required=True)
    pa.add_argument("--soul",      required=True)
    pa.add_argument("--skill",     required=True)
    pa.add_argument("--values",    default="")
    pa.add_argument("--force",     action="store_true")

    prs = sub.add_parser("reset-skill", help="Сбросить скилл агента")
    prs.add_argument("agent_id")

    sub.add_parser("gc",    help="Очистка idle-клонов")
    sub.add_parser("flush", help="ОПАСНО: удалить все ключи")
    sub.add_parser("souls", help="Показать доступные души")

    pgr = sub.add_parser("gap-resolve", help="Разрешить запросы на недостающие души")
    pgr.add_argument("--action", choices=["create","upload","skip"], default="create")
    pgr.add_argument("--files",  nargs="*", default=[],
                     help="Пути к soul.yaml файлам (для action=upload)")

    pdl = sub.add_parser("dlq-list",  help="Задачи в Dead Letter Queue")
    pdl.add_argument("--count", type=int, default=20)

    pdr = sub.add_parser("dlq-retry", help="Повторить задачу из DLQ")
    pdr.add_argument("task_id")

    pel = sub.add_parser("evolution-log", help="История эволюции души направления")
    pel.add_argument("direction")
    pel.add_argument("--n", type=int, default=10)

    psi = sub.add_parser("soul-index", help="Динамический каталог душ")
    psi.add_argument("--rebuild", action="store_true")
    psi.add_argument("--llm",     action="store_true", help="LLM-индексация (точнее, медленнее)")
    psi.add_argument("--type",    choices=["agent","orchestrator"], default=None)

    pjh = sub.add_parser("judge-history",
                         help="История попыток Judge (rework loop) по task_id")
    pjh.add_argument("task_id")

    sub.add_parser("config", help="Показать эффективную конфигурацию (после env-override)")
    sub.add_parser("health-check", help="Проверить Redis + Hermes-proxy + SOULs/")
    sub.add_parser("lifecycle",    help="Текущий refcount direction'ов + живые worker PIDs")

    pd = sub.add_parser("diag", help="Полная диагностика задачи (status/decomp/notify/workers)")
    pd.add_argument("task_id")

    pn = sub.add_parser("notify-now",
                        help="Зарегистрировать push для уже отправленной задачи")
    pn.add_argument("task_id")
    pn.add_argument("target", help="tg:<chat_id> | slack:<channel> | webhook:<url> | log:")

    pa = sub.add_parser("ask", help="End-to-end: submit + stream прогресса + финал")
    pa.add_argument("task", help="Текст задачи")
    pa.add_argument("--format", choices=["text", "json"], default="text",
                    help="text — для человека; json — для парсинга ботом")
    pa.add_argument("--timeout", type=int, default=600,
                    help="Сколько секунд ждать финала (default 600)")
    pa.add_argument("--poll", type=float, default=1.5,
                    help="Интервал опроса Redis (default 1.5 сек)")

    pw = sub.add_parser("wait", help="Дождаться финала уже отправленной задачи")
    pw.add_argument("task_id")
    pw.add_argument("--timeout", type=int, default=600)
    pw.add_argument("--poll", type=float, default=1.5)

    pta = sub.add_parser("tasks-active",
                         help="Список активных (выполняющихся) задач")
    pta.add_argument("--json", action="store_true")

    ptd = sub.add_parser("tasks-by-date",
                         help="Задачи за дату (YYYY-MM-DD, 'today', 'yesterday')")
    ptd.add_argument("date")
    ptd.add_argument("--json", action="store_true")

    args = p.parse_args()
    cmds = {
        "bootstrap":     cmd_bootstrap,
        "start":         cmd_start,
        "stop":          cmd_stop,
        "status":        cmd_status,
        "submit":        cmd_submit,
        "result":        cmd_result,
        "watch":         cmd_watch,
        "agents":        cmd_agents,
        "metrics":       cmd_metrics,
        "agent-info":    cmd_agent_info,
        "add-agent":     cmd_add_agent,
        "reset-skill":   cmd_reset_skill,
        "gc":            cmd_gc,
        "flush":         cmd_flush,
        "souls":         cmd_souls,
        "gap-resolve":   cmd_gap_resolve,
        "dlq-list":      cmd_dlq_list,
        "dlq-retry":     cmd_dlq_retry,
        "evolution-log": cmd_evolution_log,
        "soul-index":    cmd_soul_index,
        "judge-history": cmd_judge_history,
        "config":        cmd_config,
        "health-check":  cmd_health,
        "lifecycle":     cmd_lifecycle,
        "diag":          cmd_diag,
        "notify-now":    cmd_notify_now,
        "ask":           cmd_ask,
        "wait":          cmd_wait,
        "tasks-active":  cmd_tasks_active,
        "tasks-by-date": cmd_tasks_by_date,
    }
    cmds[args.command](args)


if __name__ == "__main__":
    main()
