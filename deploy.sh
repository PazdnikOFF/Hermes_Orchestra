#!/usr/bin/env bash
# deploy.sh — синхронизировать локальный orchestra с установкой в Hermes.
#
# Использование (запускать НА HERMES-ХОСТЕ):
#   bash deploy.sh
#   bash deploy.sh /custom/path/to/hermes-agent
#
# Что делает:
#   1. Останавливает оркестр и убивает hung subprocess'ы
#   2. Копирует modules/, scripts/, SKILL.md, tools/orchestra_tool.py
#   3. Создаёт ~/.hermes/orchestra.env если его нет
#   4. Health-check: Redis + Hermes-proxy + SOULs/
#   5. Запускает оркестр заново
#
# Идемпотентен — можно гонять после каждой правки.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HERMES="${1:-$HOME/.hermes/hermes-agent}"
ORCHESTRA_DIR="$HERMES/skills/orchestra"
ENV_FILE="$HOME/.hermes/orchestra.env"

if [[ ! -d "$ORCHESTRA_DIR" ]]; then
    echo "ОШИБКА: $ORCHESTRA_DIR не найден."
    echo "Сначала прогони install.sh"
    exit 1
fi

echo "=== Orchestra deploy ==="
echo "  source: $SCRIPT_DIR"
echo "  target: $ORCHESTRA_DIR"
echo ""

# 1. Стоп
echo "[1/5] Остановка оркестра…"
python3 "$ORCHESTRA_DIR/scripts/orchestra_ctl.py" stop 2>/dev/null || true
pkill -9 -f "modules.workers" 2>/dev/null || true
pkill -9 -f "modules.notifier" 2>/dev/null || true
sleep 1

# 2. Копирование файлов
echo "[2/5] Копирование исходников…"
cp -r "$SCRIPT_DIR/modules/." "$ORCHESTRA_DIR/modules/"
cp -r "$SCRIPT_DIR/SOULs"     "$ORCHESTRA_DIR/" 2>/dev/null || true
cp    "$SCRIPT_DIR/skills/orchestra/SKILL.md"            "$ORCHESTRA_DIR/"
cp    "$SCRIPT_DIR/skills/orchestra/scripts/orchestra_ctl.py" "$ORCHESTRA_DIR/scripts/"
# Hermes-тул: КРИТИЧНО для регистрации orchestra_submit/tasks_active/... .
# Копируем громко (без `|| true`) — set -e оборвёт деплой, если не удалось,
# чтобы тихий сбой не оставлял инструменты Hermes незарегистрированными.
mkdir -p "$HERMES/tools"
cp    "$SCRIPT_DIR/tools/orchestra_tool.py" "$HERMES/tools/orchestra_tool.py"
# orchestra_tgbot.py — опционален (TG-бот). Копируем если присутствует.
[[ -f "$SCRIPT_DIR/tools/orchestra_tgbot.py" ]] \
    && cp "$SCRIPT_DIR/tools/orchestra_tgbot.py" "$HERMES/tools/orchestra_tgbot.py"
cp    "$SCRIPT_DIR/ARCHITECTURE.md" "$ORCHESTRA_DIR/" 2>/dev/null || true
echo "    OK (tool → $HERMES/tools/orchestra_tool.py)"

# 3. Env-файл если нет
if [[ ! -f "$ENV_FILE" ]]; then
    echo "[3/5] Создаю $ENV_FILE…"
    cat > "$ENV_FILE" <<'EOF'
REDIS_URL=redis://localhost:6379
ORCHESTRA_MODEL=grok-4.3
OPENAI_BASE_URL=http://localhost:8645/v1
OPENAI_API_KEY=hermes-proxy
ORCHESTRA_JUDGE_PASS_THRESHOLD=0.79
ORCHESTRA_JUDGE_MAX_ITER=5
ORCHESTRA_USE_EMBEDDINGS=0
# Шаблон команды доставки push-уведомлений; поправь под свой `hermes send`:
ORCHESTRA_NOTIFY_CMD=hermes send {channel} {ident} --file {text_file}
EOF
    echo "    Создан. Проверь и поправь под себя:"
    echo "    cat $ENV_FILE"
else
    echo "[3/5] $ENV_FILE уже есть, не трогаю."
fi

# 4. Health check
echo "[4/5] Проверка инфраструктуры…"
set +e
python3 "$ORCHESTRA_DIR/scripts/orchestra_ctl.py" health-check
HC=$?
set -e
if [[ $HC -ne 0 ]]; then
    echo ""
    echo "Health-check провален. Исправь и запусти deploy ещё раз ИЛИ старт вручную:"
    echo "  python3 $ORCHESTRA_DIR/scripts/orchestra_ctl.py start &"
    exit 1
fi

# 5. Старт
echo ""
echo "[5/5] Запуск…"
nohup python3 "$ORCHESTRA_DIR/scripts/orchestra_ctl.py" start \
    > "$HOME/.hermes/orchestra.log" 2>&1 &
sleep 3

echo ""
echo "=== Готово ==="
echo "  Логи воркеров:    tail -f ~/.hermes/orchestra.log"
echo "  Логи subprocess:  tail -f ~/.hermes/orchestra-workers.log"
echo "  Тест:             python3 $ORCHESTRA_DIR/scripts/orchestra_ctl.py ask \"Напиши is_prime(n)\""
