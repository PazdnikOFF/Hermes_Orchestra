"""
modules/config.py — единый источник конфигурации оркестра.

ВСЕ настройки живут здесь. Ни один другой модуль не читает os.environ
напрямую — только через этот модуль.

ПРАВКИ ДЛЯ СЛЕДУЮЩЕЙ ИТЕРАЦИИ:
  - Добавь новую настройку сюда + в orchestra_ctl.py (команда config set/get)
  - Значения можно переопределить через Redis ключ orchestra:config
    (они загружаются поверх env-переменных при старте)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# ── Pre-load env file ─────────────────────────────────────────────────────
# Единый источник правды для всех процессов оркестра (Senior, Judge, Watchdog,
# AgentWorker subprocess'ы, orchestra_ctl). Один раз положил OPENAI_BASE_URL,
# ORCHESTRA_MODEL и т.п. в этот файл — все компоненты видят.
#
# Формат: KEY=VALUE на строку, # — комментарии, ничего экзотического.
# Уже существующие os.environ значения НЕ перетираем — env shell приоритетнее.

_ENV_FILE = Path(os.environ.get("ORCHESTRA_ENV_FILE",
                                os.path.expanduser("~/.hermes/orchestra.env")))


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            key, _, val = s.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val
    except Exception:
        pass


_load_env_file(_ENV_FILE)


@dataclass
class OrchestraConfig:
    # ── Redis ─────────────────────────────────────────────────────────────
    redis_url: str = "redis://localhost:6379"

    # ── LLM ───────────────────────────────────────────────────────────────
    # Дефолты ориентированы на Hermes-proxy (xai-oauth → Grok).
    # Запусти `hermes proxy start --provider xai` и оркестр заработает без env.
    # Любое значение можно переопределить через env-переменные ниже.
    model: str = "grok-4.3"
    # OpenAI SDK требует non-empty api_key; Hermes-proxy его игнорирует
    # и подставляет настоящий OAuth-токен из credential_pool.
    openai_api_key: str = "hermes-proxy"
    openai_base_url: str = "http://localhost:8080/v1"
    anthropic_api_key: str = ""

    # ── Embeddings (опциональная маршрутизация душ, итерация 4) ───────────
    embedding_model: str = "text-embedding-3-small"
    use_embeddings: bool = False  # включить → SoulRegistry scoring через cosine

    # ── Инструменты агентов (итерация 8): сеть + файлы, без shell ─────────
    # Агент через function-calling Grok'а реально фетчит URL и пишет файлы
    # проекта в изолированный workspace. Выключить → ORCHESTRA_AGENT_TOOLS=0.
    enable_agent_tools: bool = True
    # run_shell: агент сам запускает код/тесты для самопроверки. ВНИМАНИЕ:
    # это исполнение LLM-команд под пользователем оркестра (не песочница) —
    # есть denylist катастрофических команд, но не полная изоляция.
    # Выключить → ORCHESTRA_SHELL_TOOL=0.
    enable_shell_tool: bool = True
    agent_workspace_dir: str = "~/.hermes/orchestra_workspaces"
    # макс. циклов tool-call на один вызов агента. Многокомпонентная система
    # (парсер+БД+API+бот+Docker) требует много write_file → 40, иначе агент
    # упирается в лимит и спихивает остаток в «следующие шаги».
    agent_tool_max_iters: int = 40

    # ── Пути ──────────────────────────────────────────────────────────────
    # Корень пакета (папка, в которой лежит modules/)
    package_root: Path = field(default_factory=lambda: Path(__file__).parent.parent)

    @property
    def souls_dir(self) -> Path:
        return self.package_root / "SOULs"

    # ── Автоскейлинг ──────────────────────────────────────────────────────
    slow_threshold: float = 2.5     # latency > median * slow_threshold → клонировать
    idle_ttl: int = 30              # секунд простоя клона до удаления
    max_clones: int = 4             # максимум клонов на один медленный агент
    watchdog_interval: int = 10     # секунд между проверками метрик

    # ── Эволюция скилла ───────────────────────────────────────────────────
    skill_evolve_every: int = 5     # задач между эволюциями скилла

    # ── Notifier (push в TG/Slack/WhatsApp по завершении задачи) ──────────
    notify_poll_interval: int = 3      # секунд между сканами Redis
    # Шаблон команды доставки. Доступные placeholder'ы:
    #   {channel} → tg / slack / whatsapp
    #   {ident}   → chat_id / channel name / phone
    #   {text_file} → путь к файлу с готовым текстом уведомления
    notify_command_template: str = "hermes send {channel} {ident} --file {text_file}"

    # ── Judge ─────────────────────────────────────────────────────────────
    judge_doubt_threshold: float = 0.3  # при каком confidence Judge создаёт doubt-агента
    # Judge rework loop (итерация 4):
    # score > judge_pass_threshold → passed; иначе → rework.
    # При score == 0.79 (граничный) — НЕ passed (используется строгое >).
    judge_pass_threshold: float = 0.79
    # Каждая доработка = agent_call + judge_evaluate ПОСЛЕДОВАТЕЛЬНО. 5 итераций
    # давали до 10 серийных LLM-вызовов на один сабтаск — главный источник
    # хвоста латентности. 3 итерации + best-of fallback сохраняют качество.
    # Переопределяется через ORCHESTRA_JUDGE_MAX_ITER.
    judge_max_iterations: int = 3      # максимум доработок до финализации best-of


def load_config() -> OrchestraConfig:
    """
    Загружает конфигурацию: сначала дефолты из OrchestraConfig, потом env-override.

    Семантика env: пустая или отсутствующая переменная → используем дефолт.
    Это важно для openai_api_key — пустая строка ломает OpenAI SDK.
    """
    defaults = OrchestraConfig()

    def env_or(key: str, default: str) -> str:
        v = os.environ.get(key, "")
        return v if v else default

    return OrchestraConfig(
        redis_url=env_or("REDIS_URL", defaults.redis_url),
        model=env_or("ORCHESTRA_MODEL", defaults.model),
        openai_api_key=env_or("OPENAI_API_KEY", defaults.openai_api_key),
        openai_base_url=env_or("OPENAI_BASE_URL", defaults.openai_base_url),
        anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
        embedding_model=env_or("ORCHESTRA_EMBEDDING_MODEL", defaults.embedding_model),
        use_embeddings=os.environ.get("ORCHESTRA_USE_EMBEDDINGS", "0") == "1",
        enable_agent_tools=os.environ.get("ORCHESTRA_AGENT_TOOLS", "1") == "1",
        enable_shell_tool=os.environ.get("ORCHESTRA_SHELL_TOOL", "1") == "1",
        agent_workspace_dir=env_or("ORCHESTRA_WORKSPACE_DIR", defaults.agent_workspace_dir),
        agent_tool_max_iters=int(os.environ.get("ORCHESTRA_AGENT_TOOL_ITERS",
                                                str(defaults.agent_tool_max_iters))),
        slow_threshold=float(os.environ.get("ORCHESTRA_SLOW_THRESHOLD", str(defaults.slow_threshold))),
        idle_ttl=int(os.environ.get("ORCHESTRA_IDLE_TTL", str(defaults.idle_ttl))),
        max_clones=int(os.environ.get("ORCHESTRA_MAX_CLONES", str(defaults.max_clones))),
        watchdog_interval=int(os.environ.get("ORCHESTRA_WATCHDOG_INTERVAL", str(defaults.watchdog_interval))),
        skill_evolve_every=int(os.environ.get("ORCHESTRA_SKILL_EVOLVE_EVERY", str(defaults.skill_evolve_every))),
        judge_doubt_threshold=float(os.environ.get("ORCHESTRA_JUDGE_DOUBT", str(defaults.judge_doubt_threshold))),
        judge_pass_threshold=float(os.environ.get("ORCHESTRA_JUDGE_PASS_THRESHOLD", str(defaults.judge_pass_threshold))),
        judge_max_iterations=int(os.environ.get("ORCHESTRA_JUDGE_MAX_ITER", str(defaults.judge_max_iterations))),
        notify_poll_interval=int(os.environ.get("ORCHESTRA_NOTIFY_POLL", str(defaults.notify_poll_interval))),
        notify_command_template=env_or("ORCHESTRA_NOTIFY_CMD", defaults.notify_command_template),
    )


# Глобальный singleton — загружается один раз при импорте
# Для тестов можно подменить: config.CFG = OrchestraConfig(...)
CFG: OrchestraConfig = load_config()


# ── Глушим болтливые сторонние логгеры ─────────────────────────────────────
# httpx логирует КАЖДЫЙ LLM-запрос на INFO ("HTTP Request: POST … 200 OK").
# С tool-loop'ом (до 40 вызовов на агента) это заваливает консоль. Оставляем
# только WARNING+. Импортируется всеми процессами оркестра, поэтому централизованно.
import logging as _logging
for _noisy in ("httpx", "httpcore", "openai", "urllib3", "asyncio"):
    _logging.getLogger(_noisy).setLevel(_logging.WARNING)
