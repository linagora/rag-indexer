# End-to-end test plan: cozy-stack → RabbitMQ → rag-indexer → openRAG

These scenarios drive a real cozy-stack instance (files, assistants, admin routes), let
the stack publish its `sync` messages, and check what openRAG ends up holding: the file,
its md5sum, its workspaces, and what a search finds. They read the indexer's JSON log for
what happened on the way (retries, uploads, refused formats).

They need a real openRAG: a stub cannot tell whether a PDF was parsed or a text is found.

## Environment

Start these yourself, then fill in a copy of `env.example` (out of the repository: it holds
the openRAG key).

- openRAG at `E2E_RAG_URL`. The scenarios query it directly.
- `fault_proxy.py` in front of it, at `E2E_RAG_PROXY_URL`. The stack and the indexer reach
  openRAG through it, and it forwards everything until a failure scenario makes it fail.
  Start it with `python3 -I tests/e2e/cozy/fault_proxy.py "$E2E_RAG_URL" 8106 "$E2E_FAULT_MODE_FILE"`.
- RabbitMQ with its management plugin.
- cozy-stack with the `rag.index.topic` exchange and the proxy as its openRAG server:

  ```yaml
  rag:
    default:
      url: http://127.0.0.1:8106          # E2E_RAG_PROXY_URL
      api_key: ...                        # the key of E2E_RAG_URL
  rabbitmq:
    enabled: true
    nodes:
      default:
        enabled: true
        url: amqp://guest:guest@localhost:5672/
    exchanges:
      - name: rag.index.topic
        kind: topic
        durable: true
        declare_exchange: false
  ```

- Two fresh instances, `E2E_DOMAIN` and `E2E_FAULTS_DOMAIN`.
  The scenarios create the two `rag-index` triggers of the stack's `docs/ai.md`, with a 5 s
  debounce, when the instance has none.
- rag-indexer, logging to `E2E_INDEXER_LOG`, with short retries so that a message
  reaches the DLQ within a minute:

  ```sh
  RABBITMQ_URL=amqp://guest:guest@localhost:5672/ LOG_LEVEL=DEBUG CONCURRENCY=4 \
  RETRY_INTERVALS=5000,15000,30000 \
  RAG_BASE_URL="$E2E_RAG_PROXY_URL" RAG_API_KEY="$E2E_RAG_API_KEY" \
  uv run python -m rag_indexer >> /tmp/rag-indexer.log 2>&1
  ```

`E2E_INDEXER_START`/`E2E_INDEXER_STOP` and `E2E_RABBITMQ_START`/`E2E_RABBITMQ_STOP` are the
shell commands the failure scenarios run to stop and start them.

openRAG posts its `success`/`error` callbacks to the stack itself: when it cannot reach the
stack (a stack on a laptop and a shared openRAG), only the statuses the indexer posts
(`notsupported`, `error` from the DLQ) arrive. S26 checks the callback route on its own.

## Running

```sh
set -a; . ./e2e.env; set +a
python3 -I tests/e2e/cozy/scenarios.py          # functional, about 6 minutes
python3 -I tests/e2e/cozy/faults.py             # failures, about 9 minutes
python3 -I tests/e2e/cozy/scenarios.py S03 S15  # some of them
```

Run them one after the other: both instances share the fault proxy, so a fault of
`faults.py` would fail the functional scenarios running at the same time.

Without an openRAG, `openrag_stub.py` stands in for one: in memory, the routes the stack,
the indexer and these scenarios call, a search that matches the uploaded bytes. It cannot
tell whether openRAG parses a file. The `e2e-cozy` CI job runs `scenarios.py` this way,
against a cozy-stack built from the branch it names; `faults.py` is not run there.

```sh
python3 -I tests/e2e/cozy/openrag_stub.py 8090 "$E2E_RAG_API_KEY"   # E2E_RAG_URL=http://127.0.0.1:8090
```

Each scenario works in its own folders, named after the time of the run. Nothing is
cleaned up: purge the instances with `POST /instances/:domain/rag/purge` on the admin API
when done.

## Functional scenarios (`scenarios.py`)

| ID | Scenario | Expected |
|---|---|---|
| S01 | Assistant created on a folder that already holds files, subfolders included | workspace named after the folder, the 3 files indexed in it, one message each |
| S02 | New file in the knowledge base | indexed, found by a search |
| S03 | Content changed | PUT, new md5sum on openRAG, new text found, old text gone |
| S04 | Rename, same content | not uploaded again, still indexed |
| S05 | File created outside every knowledge base | no message, never indexed |
| S06 | File moved out of the knowledge base | deleted from openRAG |
| S07 | File moved into the knowledge base | indexed |
| S08 | Trashed, then restored | deleted, then indexed again |
| S09 | Trashed, then deleted for good | deleted |
| S10 | Subfolder (2 levels) moved out, then back | its files deleted, then indexed again |
| S11 | Knowledge base folder renamed | its files stay indexed, nothing uploaded again |
| S12 | Assistant on a subfolder of the knowledge base, then deleted | files in both workspaces, then back to one |
| S13 | Two assistants on the same folder, one deleted | workspace and files kept |
| S14 | Last assistant of a folder deleted | workspace removed, its files deleted from openRAG |
| S15 | Formats openRAG refuses (`.bin`, `.xyz`) | never downloaded, acked, `notsupported` on the stack with the right `docRev` |
| S16 | Image without `rag.index.image.enabled` | `notsupported` set by the stack, no message |
| S17 | Two files with the same content | never in the DLQ, whether openRAG deduplicates or not |
| S18 | Cozy note | sent as `.md`, its text found |
| S19 | Three updates in a row | the last content is the one indexed |
| S20 | 40 files at once | all indexed |
| S21 | 3.9 MB text file | streamed and indexed |
| S22 | PDF and DOCX | indexed, their text found |
| S23 | Admin `rag/reset` | everything published again, nothing uploaded again |
| S24 | Admin `rag/reconcile?dir_id=` | the folder's files published again, nothing uploaded again |
| S25 | Admin `rag/prune` | files still in a knowledge base stay |
| S26 | `POST /ai/index/status` | saved, older revision ignored, invalid payloads refused with 400 |
| S27 | Names with spaces, accents, apostrophes | indexed, the filename openRAG keeps is reported |

## Failure scenarios (`faults.py`)

| ID | Scenario | Expected |
|---|---|---|
| F01 | Indexer stopped | messages wait in `rag.index.q`, with no `rag_api_key`; `/ai/index/files` refused without the right token; indexer back: indexed |
| F02 | Message processed after its file was trashed | nothing indexed: the state is read when the message is processed |
| F03 | openRAG answers 503 to the indexer for a while | retried, then indexed |
| F04 | 503 past the last retry | DLQ, `error` on the stack with the right `docRev`; DLQ replayed after the fix: indexed |
| F05 | openRAG answers 403 | DLQ at once, no retry, `error`; replayed: indexed |
| F06 | openRAG down for the stack too | job fails, checkpoint kept; openRAG back: indexed with no other change |
| F07 | RabbitMQ stopped for about 30 s | publication fails, checkpoint kept; stack and indexer reconnect, indexed |
| F08 | No queue bound to the exchange | publication refused (`NO_ROUTE`), checkpoint kept; queue declared again: indexed |
| F09 | Partitions | a file of one instance is not on the partition of the other |
| F10 | RabbitMQ stopped for about 2 minutes | indexed once it is back; the next change goes through too |
| F11 | Message for a server other than `RAG_BASE_URL` | DLQ at once, neither the stack nor that server called |
