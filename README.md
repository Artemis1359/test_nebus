# Асинхронный процессинг платежей

FastAPI + Pydantic v2, SQLAlchemy 2.0 async, PostgreSQL, RabbitMQ (FastStream),
Alembic. API принимает платёж, consumer обрабатывает его через эмуляцию шлюза
и отправляет результат через webhook. Публикатор outbox работает фоновой
задачей в consumer: отдельный процесс для него не нужен.

## Запуск

Нужны Docker Engine / Docker Desktop и Docker Compose v2:

```bash
make up                    # создаёт .env, если его нет; собирает и запускает сервисы
make ps                    # показывает состояние контейнеров и опубликованные порты
make logs                  # выводит логи API и consumer в реальном времени (Ctrl+C — выход)
make down                  # останавливает сервисы, сохраняет данные
```

Без Makefile: скопируйте `.env.example` в `.env` и выполните
`docker compose up -d --build`. API автоматически применяет миграции;
consumer ждёт готовности API и RabbitMQ.

- API: `http://localhost:8000`.
- Swagger UI: [http://localhost:8000/docs](http://localhost:8000/docs).
- RabbitMQ Management: `http://localhost:15672`, логин / пароль `payments`.
- PostgreSQL: `localhost:55432`, пользователь / пароль / база `payments`.

Порты опубликованы на loopback, учётные данные предназначены для локального
запуска. Настройки и их значения по умолчанию — в `.env.example`.
`docker compose down -v` дополнительно удаляет данные.

## API

Платёжные маршруты и `/health` требуют статический ключ в `X-API-Key`.
`/docs` и `/openapi.json` доступны без ключа — согласованное исключение
для удобства проверки задания. Схема не содержит секретов. В Swagger нажмите
**Authorize**, введите ключ из `.env` и используйте **Try it out**.

### Создать платёж

`Idempotency-Key` выбирает клиент и сохраняет для повторных запросов.
В примере используется API-ключ из `.env.example`:

```bash
curl -i http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: local-development-key-change-me' \
  -H 'Idempotency-Key: order-42' \
  -H 'Content-Type: application/json' \
  -d '{
    "amount": "1500.50",
    "currency": "RUB",
    "description": "Заказ №42",
    "metadata": {"order_id": 42},
    "webhook_url": "https://merchant.example/webhooks/payments"
  }'
```

Замените пример `webhook_url` адресом своего HTTP-получателя.
Ответ — `202 Accepted`, заголовок `Location` с адресом платежа:

```json
{
  "payment_id": "c75b27e5-e440-4427-91e2-b1e8d2764f86",
  "status": "pending",
  "created_at": "2026-10-08T07:00:00Z"
}
```

### Получить платёж

```bash
curl http://localhost:8000/api/v1/payments/ВСТАВЬТЕ_PAYMENT_ID \
  -H 'X-API-Key: local-development-key-change-me'
```

Ответ содержит `payment_id`, `amount`, `currency`, `description`, `metadata`,
`webhook_url`, `status`, `created_at`, `processed_at`. Суммы сериализуются
строками для сохранения точности decimal, даты — с часовым поясом UTC.
До обработки `processed_at` равен `null`.

### Валидация и идемпотентность

- Сумма положительная, до 16 цифр перед точкой и 2 после неё.
- Валюты: `RUB`, `USD`, `EUR`; описание обязательно, до 1000 символов.
- Метаданные — JSON-объект, по умолчанию `{}`, до 16 KiB и 32 уровней вложенности.
- Webhook — HTTP(S) URL до 2048 символов, без credentials и fragment.
- `Idempotency-Key` обязателен, не может быть пустым, до 255 символов.
- Тело запроса ограничено 64 KiB. Несовместимые с PostgreSQL символы отклоняются.

Повтор с тем же ключом и нормализованным телом возвращает тот же платёж
и его текущий статус с кодом `202`; другое тело с тем же ключом — `409`.
Уникальный индекс и `INSERT ... ON CONFLICT` защищают от конкурентных дублей.
`request_hash` — SHA-256 нормализованного тела: сумма приводится к двум знакам,
ключи JSON сортируются. Он позволяет отличить повтор от ошибочного переиспользования ключа.

Ошибки: `401` — отсутствующий / неверный API-ключ; `404` — платёж не найден;
`409` — конфликт идемпотентности; `413` — превышен размер запроса;
`422` — некорректные данные или отсутствующий `Idempotency-Key`.

## Обработка и гарантии доставки

```text
POST → транзакция [payments + outbox] → 202
                         ↓
               публикатор outbox
                         ↓
             exchange payments → payments.new
                         ↓
        consumer → шлюз → статус в БД → webhook
                         ↓ ошибка отправки
          outbox с задержкой → payments.new
                         ↓ третья неудача
              exchange payments.dlx → payments.dlq
```

1. Платёж и событие outbox сохраняются одной транзакцией. API может принимать
   платежи при недоступном RabbitMQ: события дождутся восстановления брокера в БД.
2. Публикатор выбирает готовые события через `FOR UPDATE SKIP LOCKED` и ставит
   `published_at` после publisher confirm. Используются persistent-сообщения,
   `mandatory=True`, durable-очереди / exchange и Docker volumes.
3. Один обработчик, prefetch=1, single-active-consumer. Шлюз ждёт 2–5 секунд:
   90% платежей получают `succeeded`, 10% — `failed`. Оба результата идут в webhook.
4. Результат платежа коммитится до webhook. Повторная доставка не вызывает шлюз
   заново; блокировка события и `consumed_at` предотвращают повторную обработку.
5. Webhook — HTTP POST с тем же JSON, что GET платежа. Любой 2xx означает успех;
   остальные коды, сетевые ошибки и timeout запускают retry. Redirect отключён,
   timeout ограничивает всю отправку, API-ключ сервиса получателю не передаётся.
6. Всего 3 попытки, включая первую; задержки перед второй и третьей — 2 и 4 секунды.
   Retry сохраняется в outbox вместе с завершением текущего события, затем ACK.
   После третьей ошибки webhook создаётся outbox-событие для `payments.dlq`.
7. Некорректные сообщения отклоняются через `RejectMessage` и RabbitMQ DLX.
   Ошибки БД вызывают `NackMessage(requeue=True)` и не расходуют бизнес-попытки.
   Неожиданные программные ошибки уходят в DLQ для диагностики и ручного replay.

Ожидаемые исключения эмуляции шлюза также повторяются; после третьего платёж
фиксируется как `failed` и клиент получает уведомление. Ошибка доставки webhook
не меняет статус платежа: доставка учитывается отдельно в `webhook_delivered_at`.
В DLQ сохраняются идентификаторы и диагностические данные ошибки.

ACK выполняется после коммита через FastStream `AckPolicy.NACK_ON_ERROR`.
При SIGTERM consumer ждёт активную обработку до 20 секунд; Docker даёт 30 секунд.
После остановки обработчика закрываются HTTP-клиент и соединения БД.

**Гарантия — at least once.** Падение между publisher confirm и коммитом может
дать повтор события. Падение после принятия webhook, но до коммита его доставки
может дать повтор уведомления. Получателю следует дедуплицировать по
`Idempotency-Key` или `X-Payment-ID`: оба содержат стабильный ID платежа.

Для настоящего шлюза необходима его собственная идемпотентность по ID платежа.
Внутренние webhook-адреса разрешены для доверенного владельца API-ключа;
для публичного сервиса потребуются ограничения исходящих адресов. Подпись webhook,
автоматический replay DLQ и очистка истории outbox в задание не входят.

### Валюты и статусы

Осознанно используем `Currency` и `PaymentStatus` (`StrEnum`) в Python,
`VARCHAR` с `CHECK` в PostgreSQL. Это простой вариант для фиксированных наборов ТЗ:
API и БД отклоняют неизвестные значения. Сумма хранится как `Numeric(18, 2)`.

Добавление валюты требует изменения enum и миграции `CHECK`; другая точность
валюты — также пересмотра хранения и валидации суммы. При регулярно меняющемся
списке валют стоит перейти к справочнику `currencies` с кодом и точностью.

Переходы статуса: `pending → succeeded` или `pending → failed`.
Терминальный результат сохраняется. Новый статус требует изменения enum,
миграции, логики переходов и клиентского контракта; один справочник этого не решает.

## Проверка и разработка

Для локальных проверок нужны Python 3.12+, [uv](https://docs.astral.sh/uv/)
и запущенный Docker. Зависимости зафиксированы в `uv.lock`.

```bash
make install               # uv sync --frozen
make check                 # Ruff, проверка форматирования, strict mypy
make test                  # все тесты, включая E2E

uv run pytest -m e2e       # только E2E
uv run pytest -m 'not e2e'  # без E2E
# После активации .venv можно запускать просто pytest.
```

Testcontainers сам запускает временный PostgreSQL и применяет миграции.
Для E2E собирает приложение из Dockerfile, запускает RabbitMQ, API и consumer
в изолированной сети. `.env`, ручная тестовая БД и основной compose не нужны.
После тестов контейнеры, их данные и сеть удаляются; образ и build cache сохраняются.
Без Docker инфраструктурные тесты завершаются ошибкой подготовки, а не skip.

Проверки покрывают конкурентную идемпотентность, атомарность payment/outbox,
валидацию, миграции и соответствие моделей БД, подтверждение публикации,
retry, DLQ, дедупликацию и корректное завершение по SIGTERM.
E2E использует настоящий HTTP-получатель на машине разработчика через
`host.docker.internal`; нужен локальный Docker (Linux или Docker Desktop).

Локальный запуск процессов при работающих PostgreSQL и RabbitMQ:

```bash
make env
make migrate
make migration-check
make api
# В другом терминале:
make consumer
```

## Структура

```text
app/
  main.py          сборка FastAPI и жизненный цикл
  api/             HTTP-маршруты, авторизация, зависимости
  schemas/         входные и выходные модели Pydantic
  models/          SQLAlchemy-модели payments и outbox
  services/        сценарии платежей и транзакции
  integrations/    эмуляция шлюза и HTTP webhook
  workers/         один consumer и публикатор outbox
  core/            конфигурация, БД, RabbitMQ, логи, валидация
  enums/           валюты и статусы
  exceptions/      бизнес-ошибки
migrations/        Alembic
tests/            unit, интеграционные и E2E-тесты
```

HTTP-слой преобразует бизнес-ошибки в HTTP-коды; сервис не зависит от FastAPI.
Логи API и consumer идут в stdout, уровень задаётся через `LOG_LEVEL`.
Остальные команды доступны в `make help`.
