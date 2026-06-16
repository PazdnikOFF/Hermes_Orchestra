#!/usr/bin/env python3
"""
orchestra_tgbot.py — Telegram-фронтенд для оркестра.

Запускает CLI `orchestra_ctl ask` в режиме `--format json`, парсит события
и отправляет их пользователю в Telegram. Прогресс редактируется в одно и
то же сообщение чтобы не спамить чат; финальный результат уходит отдельно
(может быть длинным).

УСТАНОВКА:
  pip install python-telegram-bot==21.*  # async API

КОНФИГ (env-переменные или ~/.hermes/orchestra-tgbot.env):
  TELEGRAM_BOT_TOKEN          — обязательно
  ORCHESTRA_TGBOT_ALLOWED_IDS — необязательно, через запятую (например "12345,67890")
                                Если пусто — бот отвечает всем.
  ORCHESTRA_CTL_PATH          — путь к orchestra_ctl.py
                                (default: ~/.hermes/hermes-agent/skills/orchestra/scripts/orchestra_ctl.py)

ЗАПУСК:
  export TELEGRAM_BOT_TOKEN=123456:ABC...
  python tools/orchestra_tgbot.py

КОМАНДЫ В TG:
  /start              — приветствие + помощь
  /ask <текст>        — отправить задачу в оркестр
  <просто текст>      — то же что /ask
  /status             — статус оркестра
  /agents             — список агентов
  /history <task_id>  — история rework'ов Judge
  /cancel             — отменить текущую активную задачу пользователя
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Optional

# Загрузка .env файла бота
_BOT_ENV = Path(os.environ.get("ORCHESTRA_TGBOT_ENV",
                               os.path.expanduser("~/.hermes/orchestra-tgbot.env")))
if _BOT_ENV.exists():
    for line in _BOT_ENV.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, _, v = s.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

try:
    from telegram import Update
    from telegram.constants import ParseMode
    from telegram.ext import (
        Application, CommandHandler, MessageHandler, ContextTypes, filters,
    )
except ImportError:
    print("ERROR: python-telegram-bot not installed.\n"
          "  pip install 'python-telegram-bot==21.*'", file=sys.stderr)
    sys.exit(1)

logging.basicConfig(
    level=os.environ.get("ORCHESTRA_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("orchestra.tgbot")

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
if not TOKEN:
    print("ERROR: TELEGRAM_BOT_TOKEN env var is required.", file=sys.stderr)
    sys.exit(1)

CTL = os.environ.get(
    "ORCHESTRA_CTL_PATH",
    os.path.expanduser("~/.hermes/hermes-agent/skills/orchestra/scripts/orchestra_ctl.py"),
)
if not Path(CTL).exists():
    print(f"ERROR: orchestra_ctl.py not found at {CTL}.\n"
          "Set ORCHESTRA_CTL_PATH env var.", file=sys.stderr)
    sys.exit(1)

ALLOWED_IDS = {
    int(s.strip()) for s in os.environ.get("ORCHESTRA_TGBOT_ALLOWED_IDS", "").split(",")
    if s.strip().isdigit()
}

# Активные задачи: user_id → asyncio.subprocess.Process
_active: dict[int, asyncio.subprocess.Process] = {}


def _allowed(update: Update) -> bool:
    if not ALLOWED_IDS:
        return True
    uid = update.effective_user.id if update.effective_user else 0
    return uid in ALLOWED_IDS


def _format_progress(events: list[dict]) -> str:
    """Сводит список событий в компактный progress-блок для TG."""
    lines = []
    for e in events[-10:]:  # последние 10 событий
        ev = e.get("event", "")
        if ev == "submitted":
            lines.append(f"📨 task `{e.get('task_id','?')[:14]}`")
        elif ev == "progress":
            lines.append(f"  · {e.get('msg','')}")
        elif ev == "subtask_done":
            lines.append(
                f"  ✓ `{e.get('subtask_id','?')[-14:]}` "
                f"score={e.get('score','?')} att={e.get('attempts','?')}"
            )
        elif ev == "subtask_rework":
            lines.append(
                f"  ↻ rework `{e.get('subtask_id','?')[-14:]}` "
                f"score={e.get('score','?')}"
            )
        elif ev == "subtask_dlq":
            lines.append(f"  ✗ DLQ `{e.get('subtask_id','?')[-14:]}`")
        elif ev == "gap":
            lines.append(f"⚠ {e.get('msg','')}")
        elif ev == "timeout":
            lines.append(f"⌛ timeout {e.get('seconds','?')}s")
        elif ev == "failed":
            lines.append(f"✗ failed: {e.get('reason','')[:80]}")
    return "\n".join(lines) or "…"


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    await update.message.reply_text(
        "Hi! Я фронт оркестра.\n\n"
        "/ask <task>        — отправить задачу\n"
        "<просто текст>     — то же самое\n"
        "/status            — статус системы\n"
        "/agents            — активные агенты\n"
        "/history <task_id> — история попыток Judge\n"
        "/cancel            — отменить твою активную задачу\n"
    )


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    out = await _run_ctl_capture("status")
    await update.message.reply_text(f"```\n{out[:3500]}\n```", parse_mode=ParseMode.MARKDOWN)


async def cmd_agents(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    out = await _run_ctl_capture("agents")
    await update.message.reply_text(f"```\n{out[:3500]}\n```", parse_mode=ParseMode.MARKDOWN)


async def cmd_history(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    if not ctx.args:
        await update.message.reply_text("Usage: /history <task_id>")
        return
    out = await _run_ctl_capture("judge-history", ctx.args[0])
    await update.message.reply_text(f"```\n{out[:3500]}\n```", parse_mode=ParseMode.MARKDOWN)


async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    uid = update.effective_user.id
    proc = _active.get(uid)
    if not proc or proc.returncode is not None:
        await update.message.reply_text("Нет активной задачи.")
        return
    try:
        proc.terminate()
        await update.message.reply_text("Cancelled.")
    except Exception as exc:
        await update.message.reply_text(f"Не удалось отменить: {exc}")


async def cmd_ask(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    text = " ".join(ctx.args) if ctx.args else ""
    if not text:
        await update.message.reply_text("Usage: /ask <текст задачи>")
        return
    await _process_ask(update, text)


async def msg_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Любое текстовое сообщение трактуем как /ask."""
    if not _allowed(update):
        return
    text = update.message.text.strip()
    if not text:
        return
    await _process_ask(update, text)


async def _process_ask(update: Update, task_text: str) -> None:
    uid = update.effective_user.id

    if uid in _active and _active[uid].returncode is None:
        await update.message.reply_text(
            "У тебя уже идёт задача. /cancel чтобы прервать."
        )
        return

    progress_msg = await update.message.reply_text("📨 Отправляю в оркестр…")

    cmd = [sys.executable, CTL, "ask", task_text, "--format", "json",
           "--timeout", "900", "--poll", "1.5"]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    _active[uid] = proc

    events: list[dict] = []
    final_result: Optional[str] = None
    last_edit_text = ""
    last_edit_at = asyncio.get_event_loop().time()

    try:
        assert proc.stdout is not None
        async for raw in proc.stdout:
            try:
                evt = json.loads(raw.decode().strip())
            except json.JSONDecodeError:
                continue
            events.append(evt)

            if evt.get("event") == "final":
                final_result = evt.get("result", "")
                continue

            # Throttle edits — не чаще раз в 1.5 сек, и только если текст изменился
            now = asyncio.get_event_loop().time()
            new_text = _format_progress(events)
            if new_text != last_edit_text and now - last_edit_at > 1.5:
                try:
                    await progress_msg.edit_text(
                        new_text, parse_mode=ParseMode.MARKDOWN,
                    )
                    last_edit_text = new_text
                    last_edit_at = now
                except Exception:
                    pass

        await proc.wait()

        # Финальный апдейт прогресса
        final_progress = _format_progress(events)
        if final_progress != last_edit_text:
            try:
                await progress_msg.edit_text(
                    final_progress, parse_mode=ParseMode.MARKDOWN,
                )
            except Exception:
                pass

        if final_result:
            # Отправляем финал отдельным сообщением — может быть длинным
            await _send_long(update, final_result)
        else:
            stderr = (await proc.stderr.read()).decode()[:1000] if proc.stderr else ""
            await update.message.reply_text(
                f"⚠ Финального результата нет.\nstderr:\n```\n{stderr}\n```",
                parse_mode=ParseMode.MARKDOWN,
            )
    except Exception as exc:
        log.exception("ask failed: %s", exc)
        await update.message.reply_text(f"✗ Ошибка: {exc}")
    finally:
        _active.pop(uid, None)


async def _send_long(update: Update, text: str, chunk: int = 3800) -> None:
    """Telegram ограничивает сообщение 4096 символами — режем."""
    for i in range(0, len(text), chunk):
        await update.message.reply_text(text[i:i+chunk])


async def _run_ctl_capture(*args) -> str:
    """Запустить orchestra_ctl и вернуть stdout. Не стримит."""
    proc = await asyncio.create_subprocess_exec(
        sys.executable, CTL, *args,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    return (stdout.decode() or stderr.decode())[:3500]


def main() -> None:
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("ask",     cmd_ask))
    app.add_handler(CommandHandler("status",  cmd_status))
    app.add_handler(CommandHandler("agents",  cmd_agents))
    app.add_handler(CommandHandler("history", cmd_history))
    app.add_handler(CommandHandler("cancel",  cmd_cancel))
    # Любой текст без команды
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, msg_text))

    log.info("orchestra-tgbot запущен. allowed_ids=%s", ALLOWED_IDS or "<all>")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
