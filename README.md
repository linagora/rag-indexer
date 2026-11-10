# rag-indexer

Async RabbitMQ consumer that indexes documents into a RAG API. Listens for file upsert/delete messages, forwards them to a configurable RAG backend, and handles retries with exponential back-off via TTL delay queues.

## Architecture

```
Producer ──► RabbitMQ (topic exchange) ──► rag-indexer ──► RAG API
                  ▲                              │
                  │  TTL delay queues             │ on transient error
                  └───────── retry ◄──────────────┘
                                                  │ on fatal / exhausted
                                                  └──► DLQ
```

- **Main queue** (`rag.index.q`) — quorum queue bound to `rag.index.*`
- **Retry queues** — classic TTL queues (default: 30s, 5m, 1h) that dead-letter back to the main exchange
- **DLQ** (`rag.index.dlq`) — quorum queue for poison messages

## Quick start

```bash
# clone & install
uv sync

# copy env and fill in RAG credentials
cp env.example .env

# run the consumer
uv run python -m rag_indexer
```

## Configuration

All settings are via environment variables (see `env.example`):

| Variable | Default | Description |
|---|---|---|
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | AMQP connection string |
| `RAG_BASE_URL` | — | RAG API base URL, for `scripts/producer.py` only (the consumer reads it from each message) |
| `RAG_API_KEY` | — | RAG API bearer token, for `scripts/producer.py` only |
| `CONCURRENCY` | `1` | Max messages processed in parallel |
| `HTTP_TIMEOUT` | `60` | HTTP request timeout (seconds) |
| `CALLBACK_TIMEOUT` | `5` | Timeout of a status callback to cozy-stack (seconds) |
| `RETRY_INTERVALS` | `30000,300000,3600000` | Comma-separated retry delays (ms) |
| `LOG_LEVEL` | `INFO` | Logging level |

## Message format

The body is a JSON object, the way cozy-stack publishes it (no custom AMQP headers):

| Field | Required | Description |
|---|---|---|
| `action` | yes | `upsert`, `delete` or `sync` |
| `partition`, `file_id` | yes | Where the file lives in openRAG |
| `rag_base_url`, `rag_api_key` | yes | The openRAG server of the instance |
| `name` | upsert | File name: openRAG picks its parser from the extension |
| `md5sum` | upsert | Hex digest: the file is uploaded again only when it differs from the indexed one |
| `file_url` | upsert | Self-authenticating download URL of the content |
| `doc_rev` | with `callback_url` | Revision of the file, echoed back on the status callback |
| `callback_url`, `callback_token` | no | Where the indexing status is posted, and the bearer sent with it |
| `content_type`, `doctype`, `datetime`, `app_metadata` | no | Stored with the file in openRAG |
| `state_url`, `file_token` | sync | cozy-stack route giving the file state, and its bearer |

A `sync` names the file and nothing else: the consumer reads on `state_url` what openRAG
must hold, when it processes the message. A `404` there makes it a `delete`. Otherwise
the state (`name`, `md5sum`, `workspace_ids`, `metadata`) makes it an `upsert` whose
content is downloaded from `<state_url>/content` with the same bearer, and the file's
workspace memberships on openRAG are set to `workspace_ids`. Reading the state late means
a retried message cannot undo a newer one, a deleted file in particular.

The status callback is the payload of cozy-stack's `POST /ai/index/status`.
openRAG posts `success` or `error` once it has indexed the file; the indexer itself
posts `error` when a message lands in the DLQ, and `notsupported` when openRAG refuses
the format (the message is then acked).

openRAG refuses a `callback_url` on a private address unless
`INDEXING_CALLBACK_ALLOW_PRIVATE_URLS=true`: set it on a dev stack whose cozy-stack
runs locally (`http://cozy.localhost:8080`), or every upload is answered 400.

## Docker

```bash
docker build -t rag-indexer .
docker run --env-file .env rag-indexer
```

Exposes `:8080/health` (RabbitMQ liveness) and `:8080/metrics` (Prometheus).

## Scripts

- `scripts/producer.py` — CLI to publish upsert/delete messages to the exchange
- `scripts/dlq_replay.py` — List and replay messages stuck in the DLQ

## Tests

```bash
make test        # unit tests only
make test-all    # unit + integration (requires Docker for RabbitMQ)
```
