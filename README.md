# Payments service

Микросервис асинхронной обработки платежей. Принимает платёж, обрабатывает его через эмуляцию
платёжного шлюза и сообщает результат на webhook клиента.

Стек: Python 3.14, FastAPI, Pydantic v2, SQLAlchemy 2 (async), PostgreSQL 18, RabbitMQ 4 + FastStream,
Alembic, Docker Compose.

## Архитектура

```
client ──POST──► api ──┬─► payments ─┐  одна транзакция
                       └─► outbox  ──┘  (PostgreSQL)
                                │
               outbox-relay ◄───┘  FOR UPDATE SKIP LOCKED, publisher confirms
                     │
                     ▼
     exchange "payments" ──► payments.new ──► consumer ──► шлюз (эмуляция) ──► UPDATE status
                                  ▲    │                                         │
                    TTL истёк     │    │ ошибка                                  ▼
     payments.new.retry.<N>ms ────┘    ├─► exchange "payments.retry" (попытка < 3)   webhook
                                       └─► exchange "payments.dlx" ──► payments.new.dlq
```

| Сервис         | Что делает                                                                          |
|----------------|-------------------------------------------------------------------------------------|
| `api`          | REST API, `X-API-Key`, идемпотентное создание платежа                               |
| `outbox-relay` | Переносит события из таблицы `outbox` в RabbitMQ; можно запускать несколько реплик  |
| `consumer`     | Обрабатывает платёж, отправляет подписанный webhook, делает retry и отправку в DLQ  |
| `migrate`      | Одноразово применяет миграции Alembic до старта остальных сервисов                  |
| `webhook-mock` | Демо-получатель webhook для примеров ниже, не часть сервиса                         |

Порядок старта задан в [compose.yaml](compose.yaml): сначала postgres и rabbitmq проходят
healthcheck, затем `migrate` выполняется до конца, затем стартуют `api`, `outbox-relay`
и `consumer`. Consumer дополнительно ждёт `webhook-mock`.

## Запуск

```bash
docker compose up -d --wait
```

- API и Swagger: http://localhost:8000/docs (кнопка Authorize, ключ `dev-api-key`)
- RabbitMQ management: http://localhost:15672 (`payments` / `payments`)
- Полученные webhook: http://localhost:8080/webhooks

Ключи по умолчанию подходят только для локального запуска. Свои ключи задаются в `.env`
(образец — [.env.example](.env.example)). Остальные настройки с описаниями и значениями
по умолчанию собраны в [src/payments/config.py](src/payments/config.py), имя переменной
окружения совпадает с именем поля в верхнем регистре.

## API

Все эндпоинты `/api/v1/*` требуют заголовок `X-API-Key`. `/health` и `/docs` открыты.

### Создание платежа

Сумма передаётся строкой (`"100.50"`), не больше двух знаков после запятой; JSON-числа
отклоняются, чтобы сумма не прошла через float. Заголовок `Idempotency-Key` обязателен.

```bash
curl -s -X POST localhost:8000/api/v1/payments \
  -H 'X-API-Key: dev-api-key' \
  -H 'Idempotency-Key: order-42' \
  -H 'Content-Type: application/json' \
  -d '{
        "amount": "100.50",
        "currency": "RUB",
        "description": "Заказ #42",
        "metadata": {"order_id": 42},
        "webhook_url": "http://webhook-mock:8080/webhooks"
      }'
```

```json
{"payment_id":"01a13b8e-f130-760a-9c44-91d2b8fa9457","status":"pending","created_at":"2026-10-14T17:56:26.042078Z"}
```

Ответ — `202 Accepted`. `payment_id` — UUIDv7.

Повтор запроса с тем же ключом и тем же телом возвращает тот же платёж с тем же ответом, новый
платёж не создаётся. Тот же ключ с другим телом — `409 Conflict`. Тела сравниваются
после нормализации, поэтому `"100.5"` и `"100.50"` считаются одним запросом.

### Получение платежа

```bash
curl -s localhost:8000/api/v1/payments/<payment_id> -H 'X-API-Key: dev-api-key'
```

```json
{
  "payment_id": "01a13b8e-f130-760a-9c44-91d2b8fa9457",
  "amount": "100.50",
  "currency": "RUB",
  "description": "Заказ #42",
  "metadata": {"order_id": 42},
  "status": "succeeded",
  "webhook_url": "http://webhook-mock:8080/webhooks",
  "created_at": "2026-10-14T17:56:26.042078Z",
  "processed_at": "2026-10-14T17:56:30.877131Z",
  "webhook_delivered_at": "2026-10-14T17:56:30.880707Z"
}
```

Коды ошибок: `401` — нет ключа или ключ неверный, `404` — платёж не найден,
`422` — невалидный запрос.

## Webhook

После обработки consumer отправляет `POST` на `webhook_url`:

```json
{"event": "payment.succeeded", "payment": {"payment_id": "...", "status": "succeeded", "...": "..."}}
```

`event` принимает значение `payment.succeeded` или `payment.failed`, в `payment` то же
содержимое, что отдаёт `GET /api/v1/payments/{id}`. Успехом считается любой ответ 2xx.

**Подпись.** Запрос несёт заголовки `X-Webhook-Timestamp` (unix-время) и
`X-Webhook-Signature: sha256=<hex>`. Подпись — это HMAC-SHA256 от `<timestamp>.<сырое тело>`
на общем секрете `WEBHOOK_SECRET`. Получатель пересчитывает подпись, сравнивает её за
постоянное время и отклоняет устаревшие timestamp, чтобы нельзя было повторить перехваченный
запрос:

```python
import hashlib, hmac, time


def verify(secret: bytes, timestamp: str, body: bytes, signature: str) -> bool:
    expected = (
        "sha256=" + hmac.new(secret, f"{timestamp}.".encode() + body, hashlib.sha256).hexdigest()
    )
    return abs(time.time() - int(timestamp)) <= 300 and hmac.compare_digest(expected, signature)
```

**Доставка не реже одного раза.** Webhook может прийти повторно. Заголовок `X-Webhook-Id`
(равен `payment_id`) одинаков во всех попытках, по нему получатель отбрасывает дубли.

### Демо: retry и DLQ

У `webhook-mock` три эндпоинта:

| `webhook_url`                           | Поведение                                         |
|-----------------------------------------|---------------------------------------------------|
| `http://webhook-mock:8080/webhooks`       | принимает сразу                                   |
| `http://webhook-mock:8080/webhooks/flaky` | первые две попытки — 500, третья принимается      |
| `http://webhook-mock:8080/webhooks/fail`  | всегда 500: после 3 попыток сообщение уходит в DLQ |

Ход обработки виден в `docker compose logs -f consumer webhook-mock`, а содержимое DLQ —
в management UI: очередь `payments.new.dlq`, кнопка *Get messages*.

## Гарантии доставки

**Transactional outbox.** Платёж и событие `payment.created` записываются в одной транзакции.
`outbox-relay` раз в `OUTBOX_POLL_INTERVAL_S` (0.2 с) забирает пачку неопубликованных
событий через `FOR UPDATE SKIP LOCKED` и публикует каждое:
- с publisher confirms;
- с флагом `mandatory`: сообщение, которое некуда маршрутизировать, вернётся ошибкой, а не потеряется.

Событие отмечается опубликованным в той же транзакции. При ошибке в строке остаются
`attempts` и `last_error`, публикация повторяется с backoff. Опубликованные строки удаляются
через `OUTBOX_RETENTION_DAYS` (7 дней). Частичный индекс по неопубликованным строкам
сохраняет опрос дешёвым при любом размере таблицы.

**Идемпотентность API.** Уникальный индекс на `idempotency_key` и
`INSERT … ON CONFLICT DO NOTHING`. Параллельные запросы с одним ключом сходятся к одному
платежу без гонок.

**Идемпотентность consumer.** Relay доставляет события не реже одного раза, поэтому каждый
шаг обработки пропускается, если уже выполнен:
- финальный статус не перезаписывается (`UPDATE … WHERE status = 'pending'`);
- доставленный webhook не отправляется повторно (`webhook_delivered_at`);
- шлюз получает `payment_id` как ключ идемпотентности.

**Retry и DLQ.** Отказ шлюза (10%) — это бизнес-результат: статус `failed`, webhook
отправляется, retry не делается. Retry выполняются при ошибках доставки webhook и
инфраструктуры. Всего 3 попытки (`MAX_ATTEMPTS`).

Неудачная попытка публикуется в delay-очередь `payments.new.retry.<N>ms`, а оригинал
подтверждается. Задержка экспоненциальная: `RETRY_BASE_DELAY_MS · 2^(n-1)`, то есть 1 с и 2 с.
Когда TTL истекает, сообщение возвращается в `payments.new`. После последней попытки оно уходит
в `payments.new.dlq`; в заголовках остаются номер попытки (`payments-attempt`) и последняя
ошибка (`payments-last-error`). Повтор через брокер, а не `sleep` в обработчике, не занимает
слот consumer и переживает его рестарт.

Невалидные сообщения и платежи, которых нет в БД, уходят в DLQ сразу. Все очереди quorum:
их лимит доставок отправит в DLQ и сообщение, которое раз за разом роняет обработчик.

Чтобы переиграть сообщение из DLQ, опубликуйте его тело в exchange `payments` с ключом
`payments.new` (Publish message в management UI). Обработка идемпотентна, так что повтор безопасен.

## Разработка

```bash
uv sync
uv run ruff check . && uv run ruff format --check .
uv run mypy
uv run pytest                      # все тесты; для интеграционных нужен Docker
uv run pytest -m "not integration" # только unit-тесты
```

Интеграционные тесты поднимают PostgreSQL и RabbitMQ через testcontainers и проверяют API,
relay и consumer на настоящих сервисах. Миграция создаётся так:

```bash
DATABASE_URL=postgresql+asyncpg://... uv run alembic revision --autogenerate -m "..."
```

## Ограничения и что сделать для продакшена

- `webhook_url` проверяется только на схему http(s). В продакшене нужна защита от SSRF:
  запрет приватных адресов (а демо-получатель как раз живёт в приватной сети compose).
- Один статический API-ключ на всех. Секрет webhook тоже общий; в продакшене он выдаётся
  каждому клиенту свой.
- Параллелизм consumer ограничен `CONSUMER_CONCURRENCY` (prefetch канала). Горизонтально
  масштабируются все сервисы, кроме `migrate`.
