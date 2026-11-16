import json
import os

import aiohttp
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "rag.index.topic")
ROUTING_KEY = os.getenv("ROUTING_KEY", "rag.index.*")
QUEUE_NAME = os.getenv("QUEUE_NAME", "rag.index.q")


def parse_retry_intervals() -> list[int]:
    """Parse RETRY_INTERVALS env var (comma-separated milliseconds).

    Returns default [30000, 300000, 3600000] if not set or empty.
    Raises ValueError if any value is not a positive integer.
    """
    raw = os.getenv("RETRY_INTERVALS", "").strip()
    if not raw:
        return [30_000, 300_000, 3_600_000]
    intervals = []
    for part in raw.split(","):
        val = int(part.strip())
        if val <= 0:
            raise ValueError(f"Retry interval must be positive, got {val}")
        intervals.append(val)
    return intervals


def build_retry_queues(intervals: list[int]) -> list[tuple[str, int]]:
    """Convert interval list into (queue_name, ttl_ms) tuples.

    Queue names embed TTL as human-readable label:
    - 3600000+ and divisible by 3600000 -> "Xh"
    - 60000+ and divisible by 60000 -> "Xm"
    - Otherwise -> "Xs" using ttl_ms // 1000

    Names must stay unique: retry counting reads distinct x-death entries
    keyed by queue name, so two intervals collapsing to the same label
    (e.g. 30000 and 30500 both -> "30s") would break it. Only colliding
    names get a numeric suffix; non-colliding names are unchanged.
    """
    queues = []
    seen = set()
    for ttl_ms in intervals:
        if ttl_ms >= 3_600_000 and ttl_ms % 3_600_000 == 0:
            label = f"{ttl_ms // 3_600_000}h"
        elif ttl_ms >= 60_000 and ttl_ms % 60_000 == 0:
            label = f"{ttl_ms // 60_000}m"
        else:
            label = f"{ttl_ms // 1000}s"
        name = f"rag.index.retry.{label}.q"
        i = 2
        while name in seen:
            name = f"rag.index.retry.{label}-{i}.q"
            i += 1
        seen.add(name)
        queues.append((name, ttl_ms))
    return queues


RETRY_INTERVALS = parse_retry_intervals()
RETRY_QUEUES = build_retry_queues(RETRY_INTERVALS)
MAX_RETRIES = len(RETRY_QUEUES)
DLQ_NAME = os.getenv("DLQ_NAME", "rag.index.dlq")

def parse_rag_api_keys() -> dict[str, str]:
    """Parse RAG_API_KEYS, a JSON object of openRAG base URL -> API key.

    It is also the list of the servers a message may name: reading the broker gives
    no access to openRAG, and a message cannot send a key to an arbitrary URL.
    """
    raw = os.getenv("RAG_API_KEYS", "").strip()
    if not raw:
        return {}
    keys = json.loads(raw)
    # An empty object would silently fall back to the keys of the messages.
    if not isinstance(keys, dict) or not keys or not all(
        isinstance(url, str) and url and isinstance(key, str) and key for url, key in keys.items()
    ):
        raise ValueError("RAG_API_KEYS must be a JSON object of non-empty base URLs to non-empty API keys")
    # The base URLs of the messages are compared without their trailing slash.
    return {url.rstrip("/"): key for url, key in keys.items()}


RAG_API_KEYS = parse_rag_api_keys()

CONCURRENCY = int(os.getenv("CONCURRENCY", "1"))

_HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT", "60"))
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=_HTTP_TIMEOUT_SECONDS)

# Short, so the shutdown drain never hangs on a slow or unreachable cozy webhook.
_CALLBACK_TIMEOUT_SECONDS = int(os.getenv("CALLBACK_TIMEOUT", "5"))
CALLBACK_TIMEOUT = aiohttp.ClientTimeout(total=_CALLBACK_TIMEOUT_SECONDS)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8080"))
