#!/usr/bin/env bash
# install.sh — устанавливает Orchestra v2 в Hermes Agent
#
# Использование:
#   bash install.sh                        # автоопределение HERMES_HOME
#   bash install.sh /path/to/hermes-agent  # явный путь

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HERMES_REPO="${1:-}"

echo "=== Orchestra v2 — Установка ==="
echo ""

# 1. Найти hermes-agent
if [[ -z "$HERMES_REPO" ]]; then
    for c in "$HOME/.hermes/hermes-agent" "$HOME/hermes-agent" "$(pwd)/hermes-agent" "/opt/hermes-agent"; do
        if [[ -f "$c/run_agent.py" ]]; then HERMES_REPO="$c"; break; fi
    done
fi
if [[ -z "$HERMES_REPO" || ! -f "$HERMES_REPO/run_agent.py" ]]; then
    echo "ОШИБКА: hermes-agent не найден. Передай путь: bash install.sh /path/to/hermes-agent"
    exit 1
fi
echo "Hermes repo: $HERMES_REPO"

# 2. Установить skill (включая всю структуру)
SKILL_DST="$HERMES_REPO/skills/orchestra"
echo "Установка skill → $SKILL_DST"
rm -rf "$SKILL_DST"
cp -r "$SCRIPT_DIR/skills/orchestra" "$SKILL_DST"

# 3. Скопировать modules/ и SOULs/ в skill
echo "Копирование modules/ и SOULs/ → $SKILL_DST"
cp -r "$SCRIPT_DIR/modules" "$SKILL_DST/modules"
cp -r "$SCRIPT_DIR/SOULs"   "$SKILL_DST/SOULs"

# 4. Скопировать ARCHITECTURE.md
cp "$SCRIPT_DIR/ARCHITECTURE.md" "$SKILL_DST/"

# 5. Установить tool plugin
echo "Установка tool → $HERMES_REPO/tools/orchestra_tool.py"
mkdir -p "$HERMES_REPO/tools"
cp "$SCRIPT_DIR/tools/orchestra_tool.py" "$HERMES_REPO/tools/orchestra_tool.py"
# Опциональный TG-бот
[[ -f "$SCRIPT_DIR/tools/orchestra_tgbot.py" ]] \
    && cp "$SCRIPT_DIR/tools/orchestra_tgbot.py" "$HERMES_REPO/tools/orchestra_tgbot.py"

# Патч пути в tool plugin
SKILL_ABS="$SKILL_DST"
python3 -c "
import re
path = '$HERMES_REPO/tools/orchestra_tool.py'
with open(path) as f: content = f.read()
content = re.sub(
    r'PACKAGE_ROOT = Path\(__file__\).*',
    'PACKAGE_ROOT = Path(\"$SKILL_ABS\")',
    content
)
with open(path, 'w') as f: f.write(content)
print('  tool path patched')
"

# 6. Зависимости
echo ""
echo "Установка зависимостей Python…"
pip install redis pyyaml openai anthropic --quiet --break-system-packages 2>/dev/null \
    || pip install redis pyyaml openai anthropic --quiet

# 7. Toolsets
TOOLSETS="$HERMES_REPO/toolsets.py"
if [[ -f "$TOOLSETS" ]] && ! grep -q "orchestra_submit" "$TOOLSETS"; then
    cat >> "$TOOLSETS" << 'EOF'

# Orchestra multi-agent toolset
_ORCHESTRA_TOOLS = [
    "orchestra_submit", "orchestra_status", "orchestra_result",
    "orchestra_agents", "orchestra_metrics", "orchestra_agent_info",
    "orchestra_add_agent", "orchestra_tasks_active", "orchestra_tasks_by_date",
]
EOF
    echo "Orchestra toolset добавлен в toolsets.py"
fi

echo ""
echo "=== Установка завершена ==="
echo ""
echo "Следующие шаги:"
echo ""
echo "  1. Установи переменные окружения:"
echo "       export REDIS_URL=redis://localhost:6379"
echo "       export ORCHESTRA_MODEL=gpt-4o      # или claude-sonnet-4-6"
echo "       export OPENAI_API_KEY=sk-..."
echo ""
echo "  2. Запусти Redis:"
echo "       docker run -d -p 6379:6379 redis:7-alpine"
echo ""
echo "  3. Bootstrap и запуск:"
echo "       python $SKILL_DST/scripts/orchestra_ctl.py bootstrap"
echo "       python $SKILL_DST/scripts/orchestra_ctl.py start"
echo ""
echo "  4. Отправь задачу:"
echo "       python $SKILL_DST/scripts/orchestra_ctl.py submit \"твоя задача\""
echo ""
echo "  Или просто напиши Hermes:"
echo "  'Запусти оркестр и отправь задачу: ...'"
