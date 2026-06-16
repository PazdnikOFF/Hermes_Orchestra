"""
modules/soul_registry.py — динамический каталог всех душ оркестра.

Индексирует все soul.yaml файлы из SOULs/ и хранит каталог в Redis.
Оркестраторы используют этот каталог для быстрого выбора подходящей души
по задаче — без сканирования файловой системы каждый раз.

КЛЮЧИ REDIS:
  soul_index:agent:{direction}        Hash   — SoulIndexEntry
  soul_index:all                      Set    — все direction
  soul_index:built_at                 String — timestamp последнего rebuild

КАК РАБОТАЕТ:
  1. При bootstrap → SoulRegistry.rebuild() сканирует SOULs/ и строит индекс
  2. При add-agent/add-orchestrator → SoulRegistry.index_one() добавляет запись
  3. SeniorWorker вызывает SoulRegistry.find_best_match(task) → список SoulIndexEntry
     отсортированный по релевантности
  4. Если подходящей души нет → SoulGapResolver запрашивает у пользователя

ПРАВКИ ДЛЯ СЛЕДУЮЩЕЙ ИТЕРАЦИИ:
  - Изменить алгоритм поиска → редактируй find_best_match() и _score_entry()
  - Добавить новые теги → редактируй _extract_tags()
  - Изменить TTL каталога → редактируй CATALOGUE_TTL_HOURS
  - Если LLM-индексация слишком медленная → замени на keyword-matching в _build_entry_fast()
"""

from __future__ import annotations

import json
import logging
import math
import time
from pathlib import Path
from typing import Optional

import yaml

from modules.models import SoulIndexEntry, Soul, Skill
from modules.redis_bus import get_redis
from modules import config as cfg_module

log = logging.getLogger("orchestra.soul_registry")

# Через сколько часов каталог считается устаревшим
CATALOGUE_TTL_HOURS = 24


class SoulRegistry:
    """
    Динамический каталог душ. Индекс живёт в Redis,
    перестраивается из файловой системы по запросу.
    """

    INDEX_PREFIX = "soul_index"
    ALL_KEY      = "soul_index:all"
    BUILT_AT_KEY = "soul_index:built_at"

    def __init__(self, souls_dir: Path):
        self.souls_dir = souls_dir

    # ── Публичные методы ──────────────────────────────────────────────────

    def rebuild(self, use_llm: bool = False) -> int:
        """
        Полное перестроение индекса из SOULs/.
        use_llm=True — LLM извлекает capabilities (медленно, но точнее).
        use_llm=False — keywords extraction (быстро, для bootstrap).
        Возвращает количество проиндексированных душ.
        """
        r = get_redis()
        # Очищаем старый индекс
        old_keys = r.smembers(self.ALL_KEY)
        for k in old_keys:
            r.delete(f"{self.INDEX_PREFIX}:{k}")
        r.delete(self.ALL_KEY)

        count = 0
        # Индексируем агентов
        agents_dir = self.souls_dir / "agents"
        if agents_dir.exists():
            for d in agents_dir.iterdir():
                soul_file = d / "soul.yaml"
                if d.is_dir() and soul_file.exists():
                    try:
                        entry = self._build_entry(
                            soul_file, d.name, "agent", use_llm
                        )
                        self._save_entry(entry)
                        count += 1
                    except Exception as exc:
                        log.warning("Ошибка индексации %s: %s", soul_file, exc)

        # Индексируем оркестраторов
        orch_dir = self.souls_dir / "orchestrators"
        if orch_dir.exists():
            for d in orch_dir.iterdir():
                soul_file = d / "soul.yaml"
                if d.is_dir() and soul_file.exists():
                    try:
                        entry = self._build_entry(
                            soul_file, d.name, "orchestrator", use_llm
                        )
                        self._save_entry(entry)
                        count += 1
                    except Exception as exc:
                        log.warning("Ошибка индексации %s: %s", soul_file, exc)

        r.set(self.BUILT_AT_KEY, str(time.time()))
        log.info("Soul индекс построен: %d записей", count)
        return count

    def index_one(self, soul_path: Path, direction: str, role_type: str) -> SoulIndexEntry:
        """Добавить/обновить одну запись в индексе (вызывается после add-agent)."""
        entry = self._build_entry(soul_path, direction, role_type, use_llm=False)
        self._save_entry(entry)
        return entry

    def find_best_match(
        self,
        task_description: str,
        role_type: str = "agent",
        top_n: int = 3,
    ) -> list[SoulIndexEntry]:
        """
        Найти наиболее подходящих агентов для задачи по каталогу.

        Если включён CFG.use_embeddings и у всех записей есть вектор —
        используется cosine similarity. Иначе — keyword overlap (стабильный
        fallback, не требует сети).
        """
        all_entries = self.get_all(role_type=role_type)
        if not all_entries:
            return []

        use_emb = (
            cfg_module.CFG.use_embeddings
            and all(e.embedding for e in all_entries)
        )

        if use_emb:
            from modules.llm_bridge import embed_text
            qvec = embed_text(task_description)
            if qvec:
                scored = [
                    (self._cosine(qvec, e.embedding), e) for e in all_entries
                ]
            else:
                # Embedding не получился — fallback на keyword
                scored = [(self._score_entry(e, task_description), e) for e in all_entries]
        else:
            scored = [(self._score_entry(e, task_description), e) for e in all_entries]

        scored.sort(key=lambda x: x[0], reverse=True)
        return [e for _, e in scored[:top_n]]

    @staticmethod
    def _cosine(a: list[float], b: list[float]) -> float:
        if not a or not b or len(a) != len(b):
            return 0.0
        dot  = sum(x * y for x, y in zip(a, b))
        na   = math.sqrt(sum(x * x for x in a))
        nb   = math.sqrt(sum(x * x for x in b))
        if na == 0 or nb == 0:
            return 0.0
        return dot / (na * nb)

    def find_gaps(
        self,
        required_directions: list[str],
    ) -> list[str]:
        """
        Проверяет: какие из нужных направлений не покрыты ни одной душой.
        Возвращает список недостающих направлений.
        """
        r = get_redis()
        available = {k.split(":")[-1] for k in r.smembers(self.ALL_KEY)
                     if k.startswith("agent:")}
        return [d for d in required_directions if d not in available]

    def get_all(self, role_type: Optional[str] = None) -> list[SoulIndexEntry]:
        """Все записи из индекса, опционально отфильтрованные по role_type."""
        r = get_redis()
        keys = r.smembers(self.ALL_KEY)
        entries = []
        for k in keys:
            if role_type and not k.startswith(f"{role_type}:"):
                continue
            raw = r.hgetall(f"{self.INDEX_PREFIX}:{k}")
            if raw:
                try:
                    entries.append(self._deserialise(raw))
                except Exception as exc:
                    log.warning("Ошибка десериализации %s: %s", k, exc)
        return entries

    def get_entry(self, direction: str, role_type: str = "agent") -> Optional[SoulIndexEntry]:
        """Получить конкретную запись из индекса."""
        raw = get_redis().hgetall(f"{self.INDEX_PREFIX}:{role_type}:{direction}")
        if not raw:
            return None
        try:
            return self._deserialise(raw)
        except Exception:
            return None

    def is_stale(self) -> bool:
        """True если каталог не перестраивался больше CATALOGUE_TTL_HOURS."""
        built = get_redis().get(self.BUILT_AT_KEY)
        if not built:
            return True
        return (time.time() - float(built)) > CATALOGUE_TTL_HOURS * 3600

    # ── Приватные методы ──────────────────────────────────────────────────

    def _build_entry(
        self,
        soul_path: Path,
        direction: str,
        role_type: str,
        use_llm: bool,
    ) -> SoulIndexEntry:
        raw = yaml.safe_load(soul_path.read_text(encoding="utf-8")) or {}
        soul_data  = raw.get("soul", {})
        skill_data = raw.get("skill", {})

        personality = soul_data.get("personality", "")
        description = skill_data.get("description", "")
        strengths   = skill_data.get("strengths", [])
        values      = soul_data.get("values", [])

        if use_llm:
            capabilities = self._extract_capabilities_llm(personality, description, strengths)
        else:
            capabilities = self._extract_capabilities_fast(description, strengths)

        tags = self._extract_tags(direction, values, capabilities)

        # Опционально считаем embedding (итерация 4).
        # Считаем только если флаг включён — пустой list = embedding отсутствует.
        embedding: list[float] = []
        if cfg_module.CFG.use_embeddings:
            from modules.llm_bridge import embed_text
            text = f"{direction}. {personality} {description} " + " ".join(tags)
            embedding = embed_text(text)

        return SoulIndexEntry(
            direction=direction,
            role_type=role_type,
            path=str(soul_path),
            capabilities=capabilities,
            tags=tags,
            soul_preview=personality[:120].strip(),
            skill_preview=description[:120].strip(),
            embedding=embedding,
        )

    def _extract_capabilities_fast(self, description: str, strengths: list[str]) -> list[str]:
        """Быстрое извлечение без LLM — разбивает по разделителям."""
        caps = []
        # Разбиваем description на предложения
        for sent in description.replace(".", ",").replace(";", ",").split(","):
            s = sent.strip()
            if len(s) > 5:
                caps.append(s)
        caps.extend(strengths)
        return caps[:10]

    def _extract_capabilities_llm(
        self, personality: str, description: str, strengths: list[str]
    ) -> list[str]:
        """LLM извлекает структурированный список способностей."""
        from modules.llm_bridge import call_llm
        prompt = (
            f"Personality: {personality[:300]}\n"
            f"Skill: {description[:300]}\n"
            f"Strengths: {', '.join(strengths)}\n\n"
            "Извлеки 5-8 конкретных способностей этого агента. "
            "Каждая — короткая фраза (3-7 слов). "
            "Ответь JSON-массивом строк. Только JSON."
        )
        try:
            raw = call_llm(
                "Ты индексатор агентских способностей. Отвечай только JSON.",
                [{"role": "user", "content": prompt}],
                max_tokens=256,
            ).strip()
            if raw.startswith("```"):
                raw = "\n".join(l for l in raw.split("\n") if not l.startswith("```"))
            result = json.loads(raw)
            if isinstance(result, list):
                return [str(x) for x in result[:10]]
        except Exception as exc:
            log.warning("LLM-извлечение способностей не удалось: %s", exc)
        return self._extract_capabilities_fast(description, strengths)

    def _extract_tags(
        self, direction: str, values: list[str], capabilities: list[str]
    ) -> list[str]:
        """Теги для быстрого поиска — объединяем direction, values и ключевые слова."""
        tags = {direction.lower()}
        for v in values:
            tags.add(v.lower())
        # Берём первое слово каждой capability как тег
        for cap in capabilities:
            first = cap.split()[0].lower().strip(".,;:")
            if len(first) > 2:
                tags.add(first)
        return sorted(tags)

    def _score_entry(self, entry: SoulIndexEntry, task: str) -> float:
        """
        Релевантность записи для задачи (0.0–1.0).
        Простой keyword overlap — достаточно для быстрой маршрутизации.
        Для production можно заменить на embedding similarity.
        """
        task_words = set(task.lower().split())
        score = 0.0

        # Совпадение по тегам (вес x2)
        for tag in entry.tags:
            if tag in task_words:
                score += 2.0

        # Совпадение по capabilities
        for cap in entry.capabilities:
            cap_words = set(cap.lower().split())
            overlap = len(task_words & cap_words)
            score += overlap * 0.5

        # Направление напрямую упомянуто
        if entry.direction.lower() in task.lower():
            score += 3.0

        # Нормализуем
        max_possible = 3.0 + len(entry.tags) * 2.0 + len(entry.capabilities) * 2.5
        return score / max_possible if max_possible > 0 else 0.0

    def _save_entry(self, entry: SoulIndexEntry) -> None:
        r = get_redis()
        key_short = f"{entry.role_type}:{entry.direction}"
        key_full  = f"{self.INDEX_PREFIX}:{key_short}"
        d = entry.to_dict()
        # Сериализуем списки в JSON
        d["capabilities"] = json.dumps(d["capabilities"])
        d["tags"]         = json.dumps(d["tags"])
        d["embedding"]    = json.dumps(d.get("embedding", []))
        r.hset(key_full, mapping={k: str(v) for k, v in d.items()})
        r.sadd(self.ALL_KEY, key_short)

    def _deserialise(self, raw: dict) -> SoulIndexEntry:
        d = dict(raw)
        d["capabilities"] = json.loads(d.get("capabilities", "[]"))
        d["tags"]         = json.loads(d.get("tags", "[]"))
        d["embedding"]    = json.loads(d.get("embedding", "[]"))
        d["last_indexed"] = float(d.get("last_indexed", 0))
        return SoulIndexEntry.from_dict(d)
