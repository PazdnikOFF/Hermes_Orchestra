"""
modules/soul_loader.py — загрузчик душ и скиллов из файловой системы.

Читает YAML-файлы из SOULs/ и возвращает объекты Soul и Skill.
Это единственное место где система знает о файловой структуре SOULs/.

СТРУКТУРА SOULs/:
  SOULs/
  ├── orchestrators/
  │   ├── senior/soul.yaml      ← immortal, singleton
  │   ├── middle/soul.yaml      ← шаблон, {direction} подставляется при создании
  │   ├── junior/soul.yaml      ← шаблон, {direction} подставляется при создании
  │   └── judge/soul.yaml       ← singleton
  └── agents/
      ├── math/soul.yaml
      ├── coder/soul.yaml
      ├── writer/soul.yaml
      ├── pr/soul.yaml
      ├── analyst/soul.yaml
      └── researcher/soul.yaml
      └── <custom>/soul.yaml    ← добавь папку и soul.yaml для нового агента

ПРАВКИ ДЛЯ СЛЕДУЮЩЕЙ ИТЕРАЦИИ:
  - Добавить нового агента: создай папку SOULs/agents/<name>/soul.yaml
  - Специализировать Middle под направление: создай SOULs/orchestrators/middle/<direction>/soul.yaml
    (loader проверяет его первым, затем fallback на middle/soul.yaml)
  - Формат YAML жёстко не задан — поля soul.personality, soul.values, skill.description обязательны
"""

from __future__ import annotations

import logging
from pathlib import Path
from string import Template
from typing import Optional

import yaml  # PyYAML

from modules.models import Soul, Skill

log = logging.getLogger("orchestra.soul_loader")


class SoulLoader:
    def __init__(self, souls_dir: Path):
        self.souls_dir = souls_dir

    # ── Публичные методы ───────────────────────────────────────────────────

    def load_orchestrator(
        self,
        role: str,                   # "senior" | "middle" | "junior" | "judge"
        direction: str = "",         # подставляется в шаблоны {direction}
    ) -> tuple[Soul, Skill, dict]:
        """
        Загружает душу оркестратора.
        Для middle/junior сначала ищет специализированный файл по direction,
        затем fallback на общий шаблон.
        Returns: (Soul, Skill, raw_config_dict)
        """
        # Специализированный файл: middle/research/soul.yaml
        if direction:
            specific = self.souls_dir / "orchestrators" / role / direction / "soul.yaml"
            if specific.exists():
                return self._parse(specific, direction)

        # Общий шаблон
        path = self.souls_dir / "orchestrators" / role / "soul.yaml"
        if not path.exists():
            raise FileNotFoundError(
                f"Soul file not found: {path}\n"
                f"Create SOULs/orchestrators/{role}/soul.yaml to define this orchestrator."
            )
        return self._parse(path, direction)

    def load_agent(self, direction: str) -> tuple[Soul, Skill]:
        """
        Загружает душу агента по направлению.
        Returns: (Soul, Skill)
        """
        path = self.souls_dir / "agents" / direction / "soul.yaml"
        if not path.exists():
            raise FileNotFoundError(
                f"Soul file not found: {path}\n"
                f"Create SOULs/agents/{direction}/soul.yaml to define this agent direction."
            )
        soul, skill, _ = self._parse(path, direction)
        return soul, skill

    def available_agent_directions(self) -> list[str]:
        """Возвращает список всех доступных направлений агентов из файловой системы."""
        agents_dir = self.souls_dir / "agents"
        if not agents_dir.exists():
            return []
        return [
            d.name for d in agents_dir.iterdir()
            if d.is_dir() and (d / "soul.yaml").exists()
        ]

    def get_orchestrator_meta(self, role: str) -> dict:
        """Возвращает мета-поля (immortal, singleton) из файла оркестратора."""
        path = self.souls_dir / "orchestrators" / role / "soul.yaml"
        if not path.exists():
            return {}
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        return {
            "immortal":  raw.get("immortal", False),
            "singleton": raw.get("singleton", False),
        }

    def get_judge_doubt_overlay(self) -> str:
        """Возвращает текст overlay для агентов-скептиков из Judge soul."""
        path = self.souls_dir / "orchestrators" / "judge" / "soul.yaml"
        if not path.exists():
            return ""
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        return raw.get("doubt_agent_soul_overlay", "")

    # ── Внутренние методы ─────────────────────────────────────────────────

    def _parse(self, path: Path, direction: str) -> tuple[Soul, Skill, dict]:
        """Читает YAML и возвращает (Soul, Skill, config_dict)."""
        raw_text = path.read_text(encoding="utf-8")

        # Подставляем {direction} в шаблоны (safe — не крашится на отсутствующих полях)
        if direction:
            raw_text = raw_text.replace("{direction}", direction)

        raw = yaml.safe_load(raw_text) or {}

        soul_raw  = raw.get("soul", {})
        skill_raw = raw.get("skill", {})
        config    = raw.get("config", {})

        soul = Soul(
            personality=soul_raw.get("personality", ""),
            values=soul_raw.get("values", []),
            doubt_level=float(soul_raw.get("doubt_level", 0.0)),
        )

        skill = Skill(
            version=int(skill_raw.get("version", 1)),
            description=skill_raw.get("description", ""),
            strengths=skill_raw.get("strengths", []),
            learned_patterns=skill_raw.get("learned_patterns", []),
        )

        log.debug("Loaded soul from %s (direction=%s)", path, direction)
        return soul, skill, config
