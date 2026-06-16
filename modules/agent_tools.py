"""
modules/agent_tools.py — инструменты агентов (итерация 8): сеть + файлы.

Режим без shell (выбран осознанно): агент может ходить в сеть (http_fetch) и
читать/писать файлы в ИЗОЛИРОВАННОМ workspace задачи. Произвольных команд НЕТ —
LLM-сгенерированный код не исполняется, поэтому нет риска rm -rf / утечки ключей.

Это превращает агента из «генератора текста» в исполнителя: researcher реально
скачивает страницу, coder реально пишет файлы проекта.

БЕЗОПАСНОСТЬ:
  - все файловые пути ограничены workspace (защита от ../ traversal и абсолютных)
  - http_fetch: только http/https, таймаут, лимит размера, блок приватных/loopback
    адресов (базовая защита от SSRF)

Чистые helper'ы (_safe_path, _is_allowed_url, _safe_component) покрыты self-check'ом.
"""

from __future__ import annotations

import ipaddress
import logging
import os
import re
import socket
from pathlib import Path
from urllib import request as urlreq
from urllib.parse import urlparse

log = logging.getLogger("orchestra.agent_tools")

HTTP_TIMEOUT      = 20          # сек
HTTP_READ_BYTES   = 300_000     # сколько байт читаем
HTTP_RETURN_CHARS = 40_000      # сколько символов отдаём модели (контекст)
MAX_FILE_BYTES    = 1_000_000   # лимит на write_file
MAX_LIST_FILES    = 500


# ── OpenAI-совместимые схемы инструментов ──────────────────────────────────

TOOL_SPECS = [
    {"type": "function", "function": {
        "name": "http_fetch",
        "description": ("Скачать содержимое URL (HTML/JSON/текст). Используй для "
                        "РЕАЛЬНЫХ данных из интернета — структуры страницы для "
                        "парсинга, документации, API. Не выдумывай данные — скачивай."),
        "parameters": {"type": "object", "properties": {
            "url": {"type": "string", "description": "полный http(s) URL"}},
            "required": ["url"]}}},
    {"type": "function", "function": {
        "name": "write_file",
        "description": ("Создать/перезаписать файл в рабочем каталоге проекта. "
                        "Используй чтобы строить РЕАЛЬНЫЙ проект: код, конфиги, "
                        "Dockerfile, README и т.п."),
        "parameters": {"type": "object", "properties": {
            "path":    {"type": "string", "description": "относительный путь, напр. src/main.py"},
            "content": {"type": "string"}},
            "required": ["path", "content"]}}},
    {"type": "function", "function": {
        "name": "read_file",
        "description": "Прочитать файл из рабочего каталога проекта.",
        "parameters": {"type": "object", "properties": {
            "path": {"type": "string"}}, "required": ["path"]}}},
    {"type": "function", "function": {
        "name": "list_files",
        "description": "Список всех файлов в рабочем каталоге проекта.",
        "parameters": {"type": "object", "properties": {}}}},
]


# ── Чистые helper'ы (тестируемые) ──────────────────────────────────────────

def _safe_component(name: str) -> str:
    """Санитизирует имя задачи для использования как имя папки workspace."""
    s = re.sub(r"[^A-Za-z0-9_.-]", "_", name or "")
    return s[:120] or "task"


def _safe_path(workspace: Path, relpath: str):
    """
    Резолвит relpath ВНУТРИ workspace. Возвращает Path или None, если путь
    выходит за пределы (../, абсолютный, нулевой байт).
    """
    if not relpath or relpath.startswith("/") or "\x00" in relpath:
        return None
    candidate = (workspace / relpath).resolve()
    try:
        candidate.relative_to(workspace.resolve())
    except ValueError:
        return None
    return candidate


def _is_allowed_url(url: str) -> bool:
    """http/https + хост не приватный/loopback/link-local (базовая защита от SSRF)."""
    try:
        p = urlparse(url)
    except Exception:
        return False
    if p.scheme not in ("http", "https") or not p.hostname:
        return False
    try:
        infos = socket.getaddrinfo(p.hostname, None)
    except Exception:
        return False
    for info in infos:
        try:
            ip = ipaddress.ip_address(info[4][0])
        except ValueError:
            return False
        if (ip.is_private or ip.is_loopback or ip.is_link_local
                or ip.is_reserved or ip.is_multicast):
            return False
    return True


# ── Workspace ──────────────────────────────────────────────────────────────

def ensure_workspace(task_id: str) -> Path:
    """Создаёт (если нужно) и возвращает изолированную папку проекта задачи."""
    from modules.config import CFG
    base = Path(os.path.expanduser(CFG.agent_workspace_dir))
    ws = base / _safe_component(task_id)
    ws.mkdir(parents=True, exist_ok=True)
    return ws


# ── Реализация инструментов ─────────────────────────────────────────────────

def _http_fetch(url: str) -> str:
    if not _is_allowed_url(url):
        return f"ERROR: URL отклонён (только http/https на публичные адреса): {url}"
    try:
        req = urlreq.Request(url, headers={"User-Agent": "orchestra-agent/1.0"})
        with urlreq.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            raw = resp.read(HTTP_READ_BYTES)
        text = raw.decode("utf-8", errors="replace")
        if len(text) > HTTP_RETURN_CHARS:
            text = text[:HTTP_RETURN_CHARS] + "\n…[truncated]"
        return text
    except Exception as exc:
        return f"ERROR: http_fetch failed: {exc}"


def _write_file(workspace: Path, path: str, content: str) -> str:
    p = _safe_path(workspace, path)
    if p is None:
        return f"ERROR: путь вне workspace отклонён: {path}"
    data = (content or "").encode("utf-8")
    if len(data) > MAX_FILE_BYTES:
        return f"ERROR: файл слишком большой (>{MAX_FILE_BYTES} байт)"
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        return f"OK: записано {len(data)} байт в {path}"
    except Exception as exc:
        return f"ERROR: write_file failed: {exc}"


def _read_file(workspace: Path, path: str) -> str:
    p = _safe_path(workspace, path)
    if p is None:
        return f"ERROR: путь вне workspace отклонён: {path}"
    if not p.exists():
        return f"ERROR: файл не найден: {path}"
    try:
        return p.read_text(encoding="utf-8", errors="replace")[:HTTP_RETURN_CHARS]
    except Exception as exc:
        return f"ERROR: read_file failed: {exc}"


def _list_files(workspace: Path) -> str:
    out = []
    for root, _, files in os.walk(workspace):
        for f in files:
            rel = os.path.relpath(os.path.join(root, f), workspace)
            out.append(rel)
            if len(out) >= MAX_LIST_FILES:
                break
    return "\n".join(sorted(out)) if out else "(пусто)"


def execute_tool(name: str, args: dict, workspace: Path) -> str:
    """Диспетчер. Любая ошибка возвращается строкой ERROR — не роняет агента."""
    try:
        if name == "http_fetch":
            return _http_fetch(args.get("url", ""))
        if name == "write_file":
            return _write_file(workspace, args.get("path", ""), args.get("content", ""))
        if name == "read_file":
            return _read_file(workspace, args.get("path", ""))
        if name == "list_files":
            return _list_files(workspace)
        return f"ERROR: неизвестный инструмент {name}"
    except Exception as exc:
        return f"ERROR: {exc}"
