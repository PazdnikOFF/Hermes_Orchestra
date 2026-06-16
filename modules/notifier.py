"""
modules/notifier.py — пуш-уведомления о готовых задачах.

Когда пользователь отправляет задачу через Hermes-агента в TG,
он хочет получить результат обратно в TG (или другой канал).

КАК РАБОТАЕТ:
  1. При submit `orchestra_ctl submit --notify <target>` пишется
     ключ `notify:{task_id}` со значением target (например "tg:123456789").
  2. NotifierWorker раз в N секунд сканирует все notify:*, для каждого
     проверяет results:{task_id}.status. Если done/dlq/failed — формирует
     текст уведомления и шлёт через CFG.notify_command_template.
  3. После успешной отправки notify:{task_id} удаляется.
  4. На случай если процесс упадёт — TTL ключа notify:* = 7 дней,
     при перезапуске NotifierWorker подберёт все ещё не отправленные.

ФОРМАТ TARGET:
  tg:<chat_id>            → доставка через `hermes send tg <chat_id> <text>`
  slack:<channel>         → `hermes send slack <channel> <text>`
  whatsapp:<phone>        → `hermes send whatsapp <phone> <text>`
  webhook:<url>           → POST JSON {task_id, status, result} на url
  log:                    → просто пишет в лог (для отладки)

ПРАВКИ:
  - Добавить новый канал → расширь _deliver()
  - Изменить формат текста → редактируй _format_notification()
  - Изменить периодичность → CFG.notify_poll_interval
"""

from __future__ import annotations

import json
import logging
import os
import shlex
import subprocess
import time
from typing import Optional
from urllib import request as urlrequest

from modules.redis_bus import get_redis, load_result
from modules import config as cfg_module

log = logging.getLogger("orchestra.notifier")

NOTIFY_PREFIX = "notify"             # notify:{task_id} → target string
NOTIFY_TTL    = 7 * 24 * 3600        # 7 дней
SENT_PREFIX   = "notify_sent"        # notify_sent:{task_id} → timestamp


def register_notification(task_id: str, target: str) -> None:
    """Регистрирует подписку на уведомление о готовности task_id."""
    if not target:
        return
    r = get_redis()
    r.setex(f"{NOTIFY_PREFIX}:{task_id}", NOTIFY_TTL, target)
    log.info("[notify] подписка %s → %s", task_id, target)


def is_already_sent(task_id: str) -> bool:
    return bool(get_redis().get(f"{SENT_PREFIX}:{task_id}"))


def mark_sent(task_id: str) -> None:
    get_redis().setex(f"{SENT_PREFIX}:{task_id}", NOTIFY_TTL, str(time.time()))


# ── Worker ────────────────────────────────────────────────────────────────

class NotifierWorker:
    """
    Запускается один на оркестр, рядом с Senior/Judge/Watchdog.
    Опрашивает Redis на готовые задачи с подпиской.
    """

    TERMINAL = {"done", "dlq", "failed", "partial_dlq"}

    def __init__(self, worker_id: str = "notifier"):
        self.worker_id = worker_id
        self._stop = False
        # Регистрируем сигналы для graceful shutdown
        import signal
        signal.signal(signal.SIGTERM, lambda *a: self._on_stop())
        signal.signal(signal.SIGINT,  lambda *a: self._on_stop())

    def _on_stop(self):
        # log.* в signal handler опасно — reentrant call в IO.
        self._stop = True

    def run(self):
        interval = getattr(cfg_module.CFG, "notify_poll_interval", 3)
        log.info("[Notifier] запущен (интервал=%ds)", interval)
        while not self._stop:
            try:
                self._tick()
            except Exception as exc:
                log.exception("[Notifier] tick error: %s", exc)
            time.sleep(interval)

    def _tick(self):
        r = get_redis()
        for key in r.scan_iter(match=f"{NOTIFY_PREFIX}:*", count=200):
            task_id = key.split(":", 1)[1]
            if is_already_sent(task_id):
                r.delete(key)
                continue

            data = load_result(task_id)
            status = data.get("status", "")
            if status not in self.TERMINAL:
                continue

            target = r.get(key)
            if not target:
                continue

            text = self._format_notification(task_id, data)
            ok = self._deliver(target, text, task_id, data)
            if ok:
                mark_sent(task_id)
                r.delete(key)
                log.info("[Notifier] %s → %s OK", task_id, target)
            else:
                log.warning("[Notifier] %s → %s FAIL (повторим)", task_id, target)

    # ── Форматирование ────────────────────────────────────────────────────

    def _format_notification(self, task_id: str, data: dict) -> str:
        status = data.get("status", "?")
        final  = data.get("final_result", "")
        score  = data.get("score", "")
        att    = data.get("attempts", "")
        orig   = data.get("original_task", "")

        head = f"🎼 Оркестр: задача {task_id}"
        if status == "done":
            head += " ✅"
        elif status in ("dlq", "failed"):
            head += " ✗"
        elif status == "partial_dlq":
            head += " ⚠ (часть в DLQ)"

        parts = [head, ""]
        if orig:
            parts.append(f"Запрос: {orig[:300]}")
            parts.append("")
        if score:
            parts.append(f"Score: {score}  Попыток: {att}")
            parts.append("")
        if final:
            parts.append(final)
        else:
            parts.append(f"Статус: {status}")
            if data.get("last_error"):
                parts.append(f"Ошибка: {data['last_error'][:300]}")
        return "\n".join(parts)

    # ── Доставка ──────────────────────────────────────────────────────────

    def _deliver(self, target: str, text: str, task_id: str, data: dict) -> bool:
        """Возвращает True при успешной доставке."""
        channel, _, ident = target.partition(":")

        if channel == "log":
            log.info("[Notifier:log] %s\n%s", task_id, text)
            return True

        if channel == "webhook":
            return self._deliver_webhook(ident, task_id, data)

        if channel in ("tg", "slack", "whatsapp"):
            return self._deliver_hermes_send(channel, ident, text)

        log.warning("[Notifier] неизвестный канал: %s", channel)
        return False

    def _deliver_hermes_send(self, channel: str, ident: str, text: str) -> bool:
        """
        Шлём через CLI: `hermes send <channel> <ident> <text>`.
        Команду можно переопределить через CFG.notify_command_template:
          формат с placeholder'ами {channel}, {ident}, {text_file}
        """
        tpl = getattr(
            cfg_module.CFG, "notify_command_template",
            "hermes send {channel} {ident} --file {text_file}",
        )

        # Текст пишем во временный файл — TG-сообщения могут быть длинные
        # и `hermes send` обычно поддерживает --file
        import tempfile
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, encoding="utf-8",
        ) as tmp:
            tmp.write(text)
            text_file = tmp.name

        try:
            cmd = tpl.format(channel=channel, ident=shlex.quote(ident),
                             text_file=shlex.quote(text_file))
            log.info("[Notifier] exec: %s", cmd)
            proc = subprocess.run(
                cmd, shell=True, capture_output=True, text=True, timeout=30,
            )
            if proc.returncode == 0:
                return True
            log.warning(
                "[Notifier] %s failed rc=%d stderr=%s",
                cmd, proc.returncode, proc.stderr[:200],
            )
            return False
        finally:
            try:
                os.unlink(text_file)
            except Exception:
                pass

    def _deliver_webhook(self, url: str, task_id: str, data: dict) -> bool:
        try:
            payload = json.dumps({
                "task_id":      task_id,
                "status":       data.get("status"),
                "final_result": data.get("final_result"),
                "score":        data.get("score"),
                "attempts":     data.get("attempts"),
                "original_task": data.get("original_task"),
                "last_error":   data.get("last_error"),
            }).encode()
            req = urlrequest.Request(
                url, data=payload, method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urlrequest.urlopen(req, timeout=10) as resp:
                return 200 <= resp.status < 300
        except Exception as exc:
            log.warning("[Notifier] webhook %s failed: %s", url, exc)
            return False
