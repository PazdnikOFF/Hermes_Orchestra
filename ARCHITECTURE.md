# Orchestra — Архитектурный документ

**Версия:** 5.0
**Последнее обновление:** 2026-06-16
**Назначение:** Живое описание системы для следующей итерации (Claude, ChatGPT, разработчик).
Читай этот файл ПЕРВЫМ. Обновляй после каждой итерации.

---

## Структура директорий

```
orchestra/
│
├── SOULs/                              ← ФАЙЛОВАЯ СИСТЕМА ДУШ
│   ├── orchestrators/
│   │   ├── senior/soul.yaml            ← immortal, singleton
│   │   ├── middle/soul.yaml            ← шаблон, {direction} подставляется
│   │   ├── junior/soul.yaml            ← шаблон, {direction} подставляется
│   │   └── judge/soul.yaml             ← singleton + doubt_agent_soul_overlay
│   └── agents/
│       ├── math/soul.yaml
│       ├── coder/soul.yaml
│       ├── writer/soul.yaml
│       ├── pr/soul.yaml
│       ├── analyst/soul.yaml
│       ├── researcher/soul.yaml
│       └── <new>/soul.yaml             ← добавь папку для нового направления
│
├── modules/                            ← PYTHON-МОДУЛИ (один = одна ответственность)
│   ├── config.py                       ← все настройки, единый источник
│   ├── models.py                       ← все dataclass'ы
│   ├── soul_loader.py                  ← чтение YAML из SOULs/
│   ├── soul_registry.py                ← динамический каталог душ (индекс в Redis)
│   ├── soul_gap_resolver.py            ← запрос новых душ + auto-resume (v4)
│   ├── soul_evolver.py                 ← обратная запись улучшений в YAML
│   ├── result_assembler.py             ← авто-сборка финального ответа (v4)
│   ├── redis_bus.py                    ← все операции с Redis
│   ├── llm_bridge.py                   ← все LLM-вызовы + embeddings (v4)
│   ├── agent_factory.py                ← создание/удаление агентов
│   ├── task_resilience.py              ← retry, DLQ, heartbeat, recovery (v4)
│   └── workers.py                      ← воркер-процессы + subprocess entry (v4)
│
├── tools/orchestra_tool.py             ← Hermes tool plugin
├── skills/orchestra/SKILL.md           ← Hermes skill
├── skills/orchestra/scripts/
│   └── orchestra_ctl.py                ← CLI управление
├── install.sh
└── ARCHITECTURE.md                     ← этот файл
```

---

## Иерархия агентов

```
Пользователь
    ↓  submit
Senior Orchestrator  [immortal, singleton]
    ↓  создаёт динамически по LLM-плану
Middle Orchestrator(s) [временные, один на направление]
    ↓  создаёт динамически
Junior Orchestrator(s) [временные, один на Middle]
    ↓  round-robin
Domain Agents [Math, Coder, Writer, PR, Analyst, Researcher, ...]
    ↓  результат
Judge Orchestrator [singleton]
    ↓  passed=True + улучшение
SoulEvolver → soul.yaml обновлён
    ↓  passed=True, needs_doubt_agent=True
Doubt Agent [эфемерный, 1 проверка, удаляется]
```

---

## Жизненный цикл задачи (итерация 3)

```
1. submit() → tasks:senior

2. SeniorWorker._handle():
   a. SoulRegistry.rebuild() если индекс устарел
   b. LLM: senior_plan_task() — объективно, БЕЗ ограничения на available souls
   c. SoulGapResolver.check_and_request() — проверяет что нужно vs что есть
   d. Если gap_requests → уведомляет пользователя, паркует задачу
   e. Для covered_plan → создаёт Middle, публикует подзадачи

3. (Пользователь) gap-resolve --action create / upload
   a. create: LLM генерирует soul.yaml в SOULs/agents/<direction>/
   b. upload: пользователь создаёт файл по инструкции
   c. SoulRegistry.rebuild() → индекс обновлён
   d. (TODO) Senior автоматически докомплитует задачу

4. MiddleWorker → micro-tasks → Junior → Agents

5. AgentWorker:
   - Пишет heartbeat каждые 5 сек
   - assign_task() → регистрирует назначение
   - При ошибке: schedule_retry() → exponential backoff → другой агент
   - После retry_max: DLQ

6. JudgeWorker:
   - judge_evaluate() → {score, passed, verdict, critique, needs_doubt_agent}
   - Если needs_doubt_agent → ephemeral doubt agent добавляет doubt-critique
   - Запись попытки в `judge:history:{task_id}` (для best-of)
   - Если score ≤ 0.79 и judge_iteration < 5 → отправка task обратно на
     исходного агента с rework_context=critique (iteration+1). Сама задача
     не финализируется, ResultAssembler не триггерится.
   - Если score > 0.79 (passed) → финализация done + maybe_evolve + maybe_assemble.
   - Если лимит итераций исчерпан → выбор лучшего из history (по score),
     сохранение его как result; статус done с finalized_as="best_of".

7. WatchdogWorker (каждые 10 сек):
   - ResilienceWatcher.check_all() — dead heartbeat → переназначение задач
   - Автоскейлинг медленных агентов
   - GC idle клонов
```

---

## Новые модули (итерация 3)

### soul_registry.py — динамический каталог
- Индексирует все soul.yaml в Redis (`soul_index:{type}:{direction}`)
- `find_best_match(task)` — ищет подходящую душу по keyword overlap
- `is_stale()` — проверяет TTL (по умолчанию 24 часа)
- Senior вызывает `rebuild()` при устаревшем индексе

### soul_gap_resolver.py — обработка дефицита душ
- `check_and_request()` — для каждого нужного направления проверяет индекс
- Если нет → создаёт `GapRequest` в `orchestra:gap_requests`
- `notify_user()` → текст уведомления с вариантами действий
- `resolve(CREATE)` → LLM генерирует soul.yaml на основе задачи
- `resolve(UPLOAD)` → показывает инструкцию для ручного создания

**Ключевое:** LLM при генерации новой души:
- Получает примеры похожих существующих душ ТОЛЬКО как образец формата
- НЕ копирует их — создаёт уникальную личность под конкретную задачу агента

### soul_evolver.py — эволюция душ
- Вызывается из JudgeWorker ТОЛЬКО при `verdict.passed=True`
- LLM анализирует задачу, результат и вердикт
- Определяет что добавить: new_strength, new_pattern, skill_refinement, personality_note
- Записывает обратно в soul.yaml (аддитивно, ничего не удаляет)
- Redis lock защищает от параллельной записи
- История в `soul_evolution:{direction}` (последние 50 изменений)

### task_resilience.py — устойчивость к сбоям
- `heartbeat_write(agent_id)` — AgentWorker пишет каждые 5 сек
- `heartbeat_alive(agent_id)` — TTL 30 сек
- `schedule_retry(task, error, stream)`:
  - Увеличивает `retry_count`
  - Ждёт `2^retry_count` сек (exponential backoff, max 60 сек)
  - Выбирает другого агента с живым heartbeat
  - При `retry_count >= retry_max` → DLQ
- `ResilienceWatcher.check_all()` — периодически ищет зависшие задачи

---

## Где что менять

| Хочу | Файл |
|---|---|
| Душу агента | `SOULs/agents/<direction>/soul.yaml` |
| Душу оркестратора | `SOULs/orchestrators/<role>/soul.yaml` |
| Добавить направление | Создать `SOULs/agents/<name>/soul.yaml` |
| Алгоритм поиска души | `modules/soul_registry.py` → `_score_entry()` |
| Промпт генерации душ | `modules/soul_gap_resolver.py` → `_generate_soul_llm()` |
| Что улучшается после Judge | `modules/soul_evolver.py` → `_decide_improvements()` |
| Retry логику | `modules/task_resilience.py` → `schedule_retry()` |
| Порог/backoff retry | `modules/task_resilience.py` → `_backoff_seconds()`, `HEARTBEAT_TTL` |
| Настройки | `modules/config.py` |
| Схему данных | `modules/models.py` |
| LLM-вызовы | `modules/llm_bridge.py` |
| Redis операции | `modules/redis_bus.py` |
| Создание агентов | `modules/agent_factory.py` |
| Логику воркеров | `modules/workers.py` |
| CLI команды | `skills/orchestra/scripts/orchestra_ctl.py` |

---

## CLI команды (полный список)

```bash
bootstrap          Инит Redis + агенты + индекс душ
start              Запуск воркеров
stop               Остановка
status             Статус системы
submit "<задача>"  Отправить задачу
result <task_id>   Получить результат
watch              Стрим активности
agents             Список агентов
metrics            Метрики латентности
agent-info <id>    Душа + скилл агента
add-agent          Добавить агента
reset-skill <id>   Сбросить скилл агента
gc                 GC idle клонов
flush              ⚠️ Удалить все ключи

# Новое в v3:
gap-resolve --action create/upload/skip   Разрешить дефицит душ
dlq-list [--count N]                      Задачи в DLQ
dlq-retry <task_id>                       Переотправить из DLQ
evolution-log <direction> [--n N]         История эволюции души
soul-index [--rebuild] [--llm] [--type]   Каталог душ

# Новое в v4:
judge-history <task_id>                   История попыток Judge (rework loop)
```

---

## Известные ограничения (TODO для итерации 5)

Все 5 TODO итерации 3 закрыты. Новый список:

### TODO: Партиальная парковка
Сейчас паркуется только полностью заблокированная задача (`covered_plan==[]`).
В режиме partial запущенные сабтаски выполняются, но дефицитные направления
не добавляются после gap-resolve. **Нужно:** при republish-merge добавлять
сабтаски новых направлений к существующему `decomposition` без дубликатов.

### TODO: XPENDING claim после рестарта
`AgentWorker.read_tasks` использует курсор `>` — pending-сообщения, не
acked'нутые до рестарта, теряются. `recover_on_startup` спасает только
через ASSIGN_PREFIX, но не для задач которые агент даже не успел прочитать.
**Нужно:** при старте AgentWorker делать `XPENDING` + `XCLAIM` на своей группе.

### TODO: Реальный delayed retry
`schedule_retry` блокирует через `time.sleep` — воркер простаивает.
**Нужно:** delayed queue через Redis ZSET (score=ready_at, member=task_json)
+ отдельный поток, который перебрасывает задачи в основной stream
по достижению ready_at.

### TODO: Embedding rebuild по diff
SoulRegistry пересчитывает embeddings для всех душ при каждом rebuild,
даже если изменилась одна. **Нужно:** хранить хэш yaml-файла, считать
embedding только при изменении.

### TODO: Метрики авто-сборки и DLQ-алерты
ResultAssembler пишет статус, но нет общего дашборда:
сколько задач собрано, сколько в waiting_for_souls/dlq/partial_dlq.
**Нужно:** CLI команда `pipeline-stats` + опциональный webhook при DLQ.

---

## Правки по итерациям

### Итерация 1
- Monolith: всё в orchestra_core.py
- Souls захардкожены в Python

### Итерация 2
- SOULs/ как файловая система YAML
- Модульная структура (6 модулей)
- Senior immortal
- AgentFactory

### Итерация 3 (текущая)
**Добавлено:**
- `soul_registry.py` — динамический каталог с индексом в Redis
- `soul_gap_resolver.py` — AI-генерация / ручная загрузка недостающих душ
- `soul_evolver.py` — обратная запись улучшений в soul.yaml после Judge
- `task_resilience.py` — retry (exponential backoff), DLQ, heartbeat, ResilienceWatcher
- Senior оценивает задачу объективно (без ограничения на available directions)
- AgentWorker пишет heartbeat, при ошибке вызывает schedule_retry
- JudgeWorker при passed=True вызывает SoulEvolver
- CLI: gap-resolve, dlq-list, dlq-retry, evolution-log, soul-index
- Task: поля retry_count, retry_max, last_error, assigned_agent_id
- models.py: SoulIndexEntry, SoulGapAction, TaskStatus.RETRYING/DLQ

### Итерация 4 (текущая)
**Закрыты все 5 TODO итерации 3.**

**Добавлено:**
- `result_assembler.py` — авто-сборка финального ответа.
  Триггерится из `JudgeWorker._evaluate` после сохранения `done` сабтаска.
  Атомарный lock + повторная проверка `final_result` — защита от гонки.
  После сборки вызывается `factory.cleanup_dynamic_orchestrators()`.
- `Task.parent_task_id` — корневая задача пользователя.
  Senior проставляет при создании сабтасков, Middle — при микрозадачах.
  JudgeWorker читает для триггера ResultAssembler.
- `SoulGapResolver.park_task / _unpark_task / _maybe_republish_parked_tasks` —
  парковка задачи в `orchestra:parked_tasks`. После `gap-resolve` resolver
  группирует resolved-запросы по `task_id`, проверяет `_all_gaps_resolved_for_task`,
  и при полном покрытии переотправляет задачу в `tasks:senior` с тем же `task_id`.
  Парковка только для полностью заблокированных задач (covered_plan==[]),
  чтобы не задублировать сабтаски partial-режима.
- `_spawn_worker_subprocess` + `__main__` entry в `workers.py` — Junior, AgentWorker
  и клоны теперь живут в полноценных subprocess (`Popen` с `start_new_session=True`).
  PID-ы пишутся в Redis-сет `orchestra:dynamic_pids` для `orchestra_ctl stop`.
- `ResilienceWatcher.recover_on_startup` — однократный скан ASSIGN_PREFIX:*
  при старте `WatchdogWorker` (после grace=HEARTBEAT_TTL+5 сек),
  переотправляет задачи у которых `assigned_at > 2× HEARTBEAT_TTL` и
  агент мёртв. Закрывает дыру с потерянными RUNNING-задачами после рестарта.
- `llm_bridge.embed_text` + `SoulRegistry._cosine` + `SoulIndexEntry.embedding` —
  опциональная маршрутизация через cosine similarity.
  Включается флагом `CFG.use_embeddings` (env `ORCHESTRA_USE_EMBEDDINGS=1`).
  При rebuild считается embedding каждой души; fallback на keyword-overlap.

**Мелкие правки:**
- `ResilienceWatcher._find_stuck_tasks` использует `scan_iter` вместо `keys`.
- `SeniorWorker` передаёт фактический список `available_directions` как
  hint LLM (но LLM по-прежнему свободен выходить за его пределы).
- Удалён неиспользуемый `STREAM_JUDGE_OUT`.
- `Task.from_dict` и `SoulIndexEntry.from_dict` толерантны к неизвестным полям —
  безопасное расширение схемы без миграции старых записей.
- `cmd_stop` глушит PIDs из PID-файла И из `orchestra:dynamic_pids` set.

**Новые ключи Redis:**
- `orchestra:parked_tasks` (Hash: task_id → Task JSON)
- `orchestra:dynamic_pids` (Set: PID-ы Junior/Agent/clone subprocess)
- `results:assemble_lock:{task_id}` (короткий lock на время сборки)
- `judge:history:{task_id}` (List: попытки rework-петли, TTL 1 день)

**Новые поля моделей:**
- `Task.parent_task_id: Optional[str]`
- `Task.judge_iteration: int`
- `Task.rework_context: Optional[str]`
- `SoulIndexEntry.embedding: list[float]`

**Новые настройки `config.py`:**
- `embedding_model: str = "text-embedding-3-small"`
- `use_embeddings: bool = False`
- `judge_pass_threshold: float = 0.79` (score > 0.79 → passed; ≤ 0.79 → rework)
- `judge_max_iterations: int = 5` (потом best-of по score)

**Judge rework-петля:**
- LLM возвращает `score 0..1` + actionable `critique`.
- Score ≤ 0.79 → JudgeWorker переотправляет ту же `task.id` в
  `stream_agent(source_agent_id)` с `judge_iteration += 1` и
  `rework_context = critique`. AgentWorker подмешивает критику в
  system prompt и выдаёт улучшенный результат.
- Каждая попытка пишется в `judge:history:{task_id}`.
- При исчерпании лимита (5 доработок) Judge выбирает попытку с
  максимальным score из history и сохраняет её как финальный result;
  `finalized_as = "best_of"`. После этого триггерится maybe_assemble.
- maybe_evolve вызывается ТОЛЬКО при честном passed, не при best_of.

**Новая CLI команда:**
- `orchestra_ctl judge-history <task_id>` — показывает все попытки rework
  с score/verdict/critique/preview результата.

### Итерация 5 (текущая) — производительность и корректность

Цель: убрать дубли/«путаницу агентов» и серийные накладные расходы.
Схема Redis и формат YAML НЕ менялись — миграция данных не нужна.

**Корректность (важнее перфа):**
- **Heartbeat в отдельном потоке** (`workers.py` → `AgentWorker._heartbeat_loop`).
  Был баг: LLM-вызов в `_execute` синхронный (timeout 120с) дольше
  `HEARTBEAT_TTL` (30с). Во время долгого вызова рабочий цикл не писал
  heartbeat → `ResilienceWatcher` считал агента мёртвым и переназначал задачу
  → два агента на один `task.id` → дубли в Judge. Теперь демон-поток пишет
  heartbeat и рефрешит worker-lock независимо от рабочего цикла.
  Метод `_maybe_heartbeat` удалён.

**Производительность (безопасные правки):**
- **Кэш LLM-клиентов** (`llm_bridge.py` → `_get_openai_client`/`_get_anthropic_client`).
  Раньше клиент создавался на каждый вызов (новый httpx-пул + TLS-handshake).
  Кэш по (base_url, api_key), per-process. `reset_clients()` для смены конфига.
- **Пайплайн в `AgentRegistry.all_agents`** (`redis_bus.py`). Было 1+N round-trips
  (`HKEYS` + N×`HGETALL`), стало 2. Чинит горячий путь Junior/retry/Watchdog.
- **`judge_max_iterations` 5→3** (`config.py`). Резало до 10 серийных LLM-вызовов
  на сабтаск. Best-of fallback сохранён. Override: `ORCHESTRA_JUDGE_MAX_ITER`.
- **Шорткат assemble** (`result_assembler.py`). Если суммарно один результат и нет
  DLQ — финал берётся как есть, без LLM-вызова. Teardown/статусы сохранены.
- **Эволюция скилла после publish в Judge** (`workers.py` → `AgentWorker._execute`).
  Раньше `maybe_evolve_skill` блокировал выдачу результата лишним LLM-вызовом.
  Перенесён ПОСЛЕ `publish_task`. Остаётся в процессе агента — без гонки записи
  в `agent:{id}:state` (которую дал бы вынос в отдельный воркер).
- **`count=10` для диспетчеров** Junior/Middle (`workers.py`). Батч-чтение стрима;
  consumer group гарантирует доставку ровно одному. Агенты остались на `count=1`.

**Сознательно НЕ сделано (риск дублей/петель):**
- **Delayed retry через ZSET** вместо блокирующего `time.sleep` в `schedule_retry`.
  Текущий sleep даёт неявную сериализацию (агент жив, assignment в `RETRYING`
  исключён из `_find_stuck_tasks`). ZSET расширит окно гонки между путями retry /
  rework / `recover_on_startup`, где переиспользуется один `task.id`. Внедрять
  только с exactly-once гардами: один промоутер в singleton-Watchdog, атомарный
  pop (Lua/`ZPOPMIN`), dedup по `task.id`. Оставлено как TODO итерации 3.

### Итерация 6 (текущая) — качество финала (анти-галлюцинации, анти-off-topic)

Проблема: задача-обзор финализировалась как `done`, хотя одно направление
ушло off-topic, другое не имело данных, а числа были выдуманы LLM.
Schema Redis не менялась — только additive-поля.

**Judge (`llm_bridge.py` → `judge_evaluate`):** добавлены ЖЁСТКИЕ ПРАВИЛА
поверх шкалы:
- РЕЛЕВАНТНОСТЬ: off-topic результат → score ≤ 0.2 (чтобы best-of его не вытащил).
- НЕПОДКРЕПЛЁННЫЕ ФАКТЫ: конкретные числа/даты/цитаты, которых нет в задаче и
  которые исполнитель не мог знать (нет доступа к данным) → штраф + требование
  пометить «без источника» либо убрать.

**ResultAssembler (`result_assembler.py`):** гейтинг по score/finalized_as
каждого сабтаска (читаются из `results:{sid}`):
- score < `SCORE_HARD_EXCLUDE` (0.3) → результат ИСКЛЮЧАЕТСЯ из сборки (мусор/off-topic).
- best_of / exhausted / score < `SCORE_SOFT_REVIEW` (0.5) → включается, но финал
  помечается `quality=needs_review`.
- Новые поля результата: `quality` (clean|needs_review), `quality_note`.
- При needs_review в начало `final_result` добавляется видимый баннер
  «⚠ КАЧЕСТВО: требуется проверка человеком» со списком причин.
- Если после исключения не осталось пригодных результатов → `status=failed`
  с перечнем причин (раньше просто «без полезных результатов»).

**Важное ограничение:** агенты — чистый LLM без retrieval. Для фактологических
задач (рыночные обзоры, статистика) это лечит только частично: Judge не может
проверить факт против реальности, а лишь штрафует неподкреплённые утверждения.
Полноценный research требует web/retrieval-инструмента (вариант «а», не сделан).

### Итерация 7 (текущая) — граф зависимостей направлений (DAG)

Было: Senior раскладывал задачу на НЕЗАВИСИМЫЕ направления и публиковал их все
параллельно (fan-out → LLM-сборка). Writer «синтезировал research и analysis»,
не видя их результатов — они шли параллельно. Теперь есть последовательность с
передачей данных, при сохранении параллелизма независимых веток.

**Новый модуль `dag.py`** (чистые функции, без Redis — покрыты self-check'ом):
- `normalize_plan` — канонизация плана Senior (дедуп направлений, фильтр
  depends_on до известных, без self-dep/циклов-ссылок).
- `is_acyclic` (Kahn) — детекция циклов.
- `ready_directions` — направления, готовые к публикации (все предки done).
- `has_dependencies`.

**Senior (`llm_bridge.senior_plan_task`)** — план получил поле `depends_on`
(массив направлений-предков). Нет зависимостей → как раньше (параллельно).

**`workers.py`:**
- `SeniorWorker._handle`: `normalize_plan` → если есть зависимости И граф
  ацикличен → `_handle_dag`; цикл → fallback на плоский режим (лог-варнинг).
- `_handle_dag`: `decomposition` заполняется placeholder'ами для ВСЕХ направлений
  (держат ResultAssembler от ранней сборки), публикуются только корни.
- `dag_advance` (хук из `JudgeWorker._evaluate` после финализации + страховочный
  `WatchdogWorker._dag_tick`): публикует направления, чьи предки завершены,
  **подмешивая результаты предков в контекст подзадачи**.
- `_dag_publish_direction`: exactly-once через `SADD` (гейт); сначала заменяет
  placeholder на реальные id, ПОТОМ публикует в Middle (иначе Middle-патч не
  найдёт id → зависание).
- Redis: `dag:{parent}:plan` (TTL сутки), `dag:{parent}:published` (Set).
  Чистятся в `ResultAssembler._finalize`.

**Гарантии безопасности (без петель/дедлоков/дублей):**
- Граф всегда ацикличен (иначе flat-fallback) → топологический прогресс.
- Публикация направления идемпотентна (`SADD`) → нет дублей сабтасков.
- DLQ-предок терминален → потомки всё равно освобождаются; `_dag_tick`
  гарантирует прогресс даже без Judge-события.
- **Известный edge:** краш воркера МЕЖДУ `SADD` и фактической публикацией
  оставит направление «опубликованным» без сабтасков (stall). Выбрано в пользу
  «без дублей»; восстановление такого случая — TODO.

### Итерация 8 (текущая) — инструменты агентов: сеть + файлы (executor без shell)

Было: агент = чистый chat-completion без `tools` → не мог ни скачать страницу,
ни создать файл; «данные из интернета» были галлюцинацией, а «построй систему»
сворачивалось в план. Теперь агент — исполнитель через function-calling Grok'а.

**Режим (выбран осознанно): СЕТЬ + ФАЙЛЫ, без shell.** LLM-сгенерированный код
не исполняется (нет `run_shell`) → нет риска произвольных команд на сервере.

**Новый модуль `agent_tools.py`:**
- Инструменты (OpenAI-схемы): `http_fetch`, `write_file`, `read_file`, `list_files`.
- `ensure_workspace(task_id)` — изолированная папка проекта
  (`CFG.agent_workspace_dir`, по умолчанию `~/.hermes/orchestra_workspaces/{id}`).
- Безопасность (покрыта self-check'ом, 15 кейсов):
  - `_safe_path` — все файловые операции ТОЛЬКО внутри workspace (отбой `../`,
    абсолютных путей, нулевого байта).
  - `_is_allowed_url` — только http/https на публичные адреса; блок
    loopback/private/link-local/reserved (базовая защита от SSRF).
  - лимиты: HTTP 300KB читаем / 40K символов отдаём; write_file ≤ 1MB.

**`llm_bridge.py`:** `agent_call(... workspace_id)` запускает **tool-loop**: модель
просит инструмент → хост исполняет → результат возвращается модели → повтор
(≤ `CFG.agent_tool_max_iters`=12). Модель только *просит* — выполняет хост.
При любом сбое tool-loop'а — graceful fallback на обычную текстовую генерацию.
System prompt агента (когда есть workspace) инструктирует строить по-настоящему:
фетчить реальные страницы, создавать реальные файлы. Tool-loop попутно снимает
лимит в 2048 токенов на ответ (агент пишет файлы инкрементально за много ходов).

**`workers.py`:** `AgentWorker._execute` передаёт `workspace_id = parent_task_id`
→ все направления одной задачи строят ОДИН проект и видят файлы друг друга
(вместе с DAG-пробросом результатов предков из итерации 7).

**Настройки `config.py`:** `enable_agent_tools` (env `ORCHESTRA_AGENT_TOOLS`,
дефолт вкл; claude-модели tool-loop пока не используют — fallback на текст),
`agent_workspace_dir`, `agent_tool_max_iters`.

**Что это даёт и чего ещё нет:**
- ✅ researcher реально скачивает страницы; coder реально пишет проект в workspace.
- ❌ НЕ запускает/не тестирует код (это был Docker-вариант, отклонён) — поэтому
  гарантии «собирается и работает» нет, только реальные артефакты.
- TODO: строгий Judge, проверяющий план Senior и разбивку Middle + верификация
  результата (для запуска/тестов нужен sandbox-executor — отдельная итерация).

### Итерация 9 (текущая) — завершённость: больше итераций, анти-deferral, строгий Judge

Симптом: с инструментами агент стал реально писать файлы, но останавливался на
полпути (собрал парсер+БД, а API/Telegram/Docker спихнул в «следующие шаги»).

Причины и фиксы:
- **Лимит tool-итераций был мал** (`agent_tool_max_iters` 12 → **40**): на
  многокомпонентную систему 12 ходов не хватало, агент упирался и пунтил остаток.
- **Анти-deferral в промпте агента** (`_build_agent_system`): «доведи до конца за
  проход, создай ВСЕ компоненты как файлы; отложенное в next steps/TODO считается
  НЕВЫПОЛНЕННЫМ».
- **Строгий Judge — правило ПОЛНОТА/ЗАВЕРШЁННОСТЬ** (`judge_evaluate`): если задача
  требует несколько компонентов, а часть отложена/отсутствует → score ≤ 0.4 + в
  critique перечень недостающего → rework-петля заставляет доделать.
- **Judge сверяет с реальностью** (`agent_tools.peek_workspace` → в `judge_evaluate`
  параметром `artifacts`): Judge получает фактический список файлов workspace и
  ловит расхождение «заявлено больше, чем создано». peek_workspace НЕ создаёт папку.

Это частично закрывает запрос «Judge должен быть самым требовательным»: он теперь
проверяет полноту против реальных артефактов. Проверка ПЛАНА Senior/разбивки Middle
и верификация ЗАПУСКОМ (нужен sandbox-executor) — остаются TODO.

### Итерация 10 (текущая) — shell-самопроверка + тише логи

**run_shell (самопроверка кодом).** Агент теперь может запускать код/тесты и
чинить ошибки до завершения:
- `agent_tools.run_shell(command)` — cwd=workspace, таймаут 180с, вывод ≤ 20K,
  denylist катастрофических команд (rm -rf /, sudo, systemctl, dd, curl|sh,
  fork-bomb, запись в /etc и т.п. — покрыто self-check'ом).
- `get_tool_specs()` добавляет run_shell только при `CFG.enable_shell_tool`
  (env `ORCHESTRA_SHELL_TOOL`, дефолт вкл).
- Промпт агента (при shell): «ЗАПУСТИ код, прогони тесты, ИСПРАВЬ ошибки —
  заканчивай только при exit_code=0».
- ⚠ **Это executor НА ХОСТЕ под пользователем оркестра, не песочница.** Denylist
  отсекает самое опасное, но полной изоляции нет. Для изоляции — Docker (TODO).

**Тише консоль.** В `config.py` болтливые сторонние логгеры (`httpx`, `httpcore`,
`openai`, `urllib3`, `asyncio`) понижены до WARNING. httpx логировал КАЖДЫЙ
LLM-запрос на INFO — с tool-loop'ом (до 40 на агента) это заваливало консоль.

---

## Для AI при следующих правках

1. Прочитай этот файл
2. Прочитай `modules/config.py` — настройки
3. Прочитай нужный модуль

**Не ломай без необходимости:**
- Redis ключи в `redis_bus.py` — миграция данных нужна при переименовании
- `models.py` поля `to_redis/from_redis` — должны быть симметричны
- YAML структуру SOULs/ — `soul_loader.py` ожидает конкретные поля

**Всегда обновляй этот файл** после правок.
