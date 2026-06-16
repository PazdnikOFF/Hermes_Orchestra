"""
modules/agent_factory.py — создание и удаление агентов.

Единственное место где агенты создаются.
Читает душу из SoulLoader, формирует AgentState, сохраняет в Redis.

Защита immortal-агентов: Senior и Judge помечены immortal=True —
factory.delete() игнорирует попытки их удаления.

ПРАВКИ ДЛЯ СЛЕДУЮЩЕЙ ИТЕРАЦИИ:
  - Добавить новое направление → создай SOULs/agents/<name>/soul.yaml,
    больше ничего менять не нужно
  - Изменить поведение при создании клонов → редактируй create_clone()
  - Добавить лимит на количество агентов → добавь проверку в create_agent()
"""

from __future__ import annotations

import logging
import uuid
from typing import Optional

from modules.models import AgentRole, AgentState, make_agent_id
from modules.redis_bus import AgentRegistry
from modules.soul_loader import SoulLoader
from modules import config as cfg_module

log = logging.getLogger("orchestra.agent_factory")


class AgentFactory:
    def __init__(self, soul_loader: SoulLoader):
        self.loader = soul_loader

    # ── Оркестраторы ──────────────────────────────────────────────────────

    def create_senior(self) -> AgentState:
        """
        Создаёт единственного Senior-оркестратора.
        Вызывается только из bootstrap.
        Если Senior уже есть — возвращает существующего.
        """
        existing = AgentRegistry.find_senior()
        if existing:
            log.info("Senior уже существует: %s", existing.id)
            return existing

        soul, skill, _ = self.loader.load_orchestrator("senior")
        meta = self.loader.get_orchestrator_meta("senior")

        agent = AgentState(
            id=make_agent_id("senior", "main"),
            role=AgentRole.SENIOR,
            direction="senior",
            soul=soul,
            skill=skill,
            immortal=meta.get("immortal", True),
            singleton=meta.get("singleton", True),
        )
        AgentRegistry.save(agent)
        log.info("Senior создан: %s", agent.id)
        return agent

    def create_judge(self) -> AgentState:
        """Создаёт Judge-оркестратора. Вызывается из bootstrap."""
        existing = AgentRegistry.by_role(AgentRole.JUDGE)
        if existing:
            log.info("Judge уже существует: %s", existing[0].id)
            return existing[0]

        soul, skill, _ = self.loader.load_orchestrator("judge")
        meta = self.loader.get_orchestrator_meta("judge")

        agent = AgentState(
            id=make_agent_id("judge", "main"),
            role=AgentRole.JUDGE,
            direction="judge",
            soul=soul,
            skill=skill,
            immortal=False,
            singleton=meta.get("singleton", True),
        )
        AgentRegistry.save(agent)
        log.info("Judge создан: %s", agent.id)
        return agent

    def create_middle(self, direction: str) -> AgentState:
        """
        Создаёт Middle-оркестратор для конкретного направления.
        Идемпотентно: если Middle для direction уже есть — возвращает его.
        """
        existing = [
            a for a in AgentRegistry.by_direction(direction)
            if a.role == AgentRole.MIDDLE
        ]
        if existing:
            return existing[0]

        soul, skill, config = self.loader.load_orchestrator("middle", direction)

        agent = AgentState(
            id=make_agent_id(f"middle_{direction}"),
            role=AgentRole.MIDDLE,
            direction=direction,
            soul=soul,
            skill=skill,
        )
        AgentRegistry.save(agent)
        log.info("Middle создан: %s (direction=%s)", agent.id, direction)
        return agent

    def create_junior(self, direction: str, parent_middle_id: str) -> AgentState:
        """
        Создаёт Junior-оркестратор. Идемпотентно по direction.
        """
        existing = [
            a for a in AgentRegistry.by_direction(direction)
            if a.role == AgentRole.JUNIOR
        ]
        if existing:
            return existing[0]

        soul, skill, _ = self.loader.load_orchestrator("junior", direction)

        agent = AgentState(
            id=make_agent_id(f"junior_{direction}"),
            role=AgentRole.JUNIOR,
            direction=direction,
            soul=soul,
            skill=skill,
            parent_agent_id=parent_middle_id,
        )
        AgentRegistry.save(agent)
        log.info("Junior создан: %s (direction=%s)", agent.id, direction)
        return agent

    # ── Агенты-исполнители ────────────────────────────────────────────────

    def create_agent(self, direction: str) -> AgentState:
        """
        Создаёт агента-исполнителя из SOULs/agents/<direction>/soul.yaml.
        """
        soul, skill = self.loader.load_agent(direction)

        agent = AgentState(
            id=make_agent_id(direction),
            role=AgentRole.AGENT,
            direction=direction,
            soul=soul,
            skill=skill,
        )
        AgentRegistry.save(agent)
        log.info("Агент создан: %s (direction=%s)", agent.id, direction)
        return agent

    def create_clone(self, original: AgentState) -> AgentState:
        """
        Создаёт клон агента (для автоскейлинга).
        Клон наследует soul+skill оригинала.
        """
        clone = AgentState(
            id=make_agent_id(original.direction, f"clone_{uuid.uuid4().hex[:6]}"),
            role=AgentRole.AGENT,
            direction=original.direction,
            soul=original.soul,
            skill=original.skill,
            parent_agent_id=original.id,
        )
        AgentRegistry.save(clone)

        # Регистрируем клон у родителя
        original.clone_ids.append(clone.id)
        AgentRegistry.save(original)

        log.info("Клон создан: %s ← %s", clone.id, original.id)
        return clone

    def create_doubt_agent(self, direction: str, original_soul_prompt: str) -> AgentState:
        """
        Создаёт эфемерного агента-скептика для Judge.
        Берёт базовую душу направления + накладывает doubt_overlay из Judge.
        Агент живёт ровно одну проверку.
        """
        soul, skill = self.loader.load_agent(direction)
        doubt_overlay = self.loader.get_judge_doubt_overlay()

        # Накладываем overlay на personality
        soul.personality = soul.personality.strip() + "\n\n" + doubt_overlay
        soul.doubt_level = 0.9

        agent = AgentState(
            id=make_agent_id(f"doubt_{direction}", uuid.uuid4().hex[:6]),
            role=AgentRole.AGENT,
            direction=direction,
            soul=soul,
            skill=skill,
        )
        AgentRegistry.save(agent)
        log.info("Doubt-агент создан: %s", agent.id)
        return agent

    # ── Создание начального состава ───────────────────────────────────────

    def bootstrap_default_agents(self) -> list[AgentState]:
        """
        Создаёт одного агента для каждого доступного направления.
        Вызывается из orchestra_ctl.py bootstrap.
        """
        directions = self.loader.available_agent_directions()
        agents = []
        for direction in directions:
            # Не создаём дубли
            existing = AgentRegistry.by_direction(direction)
            domain_agents = [a for a in existing if a.role == AgentRole.AGENT]
            if domain_agents:
                log.info("Агент %s уже существует, пропускаем", direction)
                continue
            agent = self.create_agent(direction)
            agents.append(agent)
        return agents

    # ── Удаление ──────────────────────────────────────────────────────────

    def delete_agent(self, agent_id: str) -> bool:
        """
        Удаляет агента. Immortal агентов не удаляет.
        Возвращает True если удалён, False если immortal.
        """
        agent = AgentRegistry.load(agent_id)
        if not agent:
            return True
        if agent.immortal:
            log.warning("Попытка удалить immortal агента %s — игнорируем", agent_id)
            return False
        AgentRegistry.delete(agent_id)
        return True

    def cleanup_dynamic_orchestrators(self) -> int:
        """
        Удаляет временные Middle и Junior оркестраторы.
        Вызывается после сборки результата задачи.
        """
        deleted = 0
        for agent in AgentRegistry.all_agents():
            if agent.role in (AgentRole.MIDDLE, AgentRole.JUNIOR) and not agent.immortal:
                AgentRegistry.delete(agent.id)
                deleted += 1
        log.info("Удалено %d временных оркестраторов", deleted)
        return deleted
