import asyncio
import json
import re

import pytest
from aiormq import AMQPConnectionError

from rag_indexer import transport
from rag_indexer.main import main
from rag_indexer.transport import _file_metadata, publish_to_dlq


async def _await_dlq_callbacks() -> None:
    """The failure callback is now a detached fire-and-forget task; let it finish
    before asserting on the session it posted to.
    """
    await asyncio.gather(*transport._callback_tasks)


def test_file_metadata_keeps_only_what_cozy_reads():
    """doc_rev/datetime/doctype only: no md5sum standing in for doc_rev, no secrets."""
    result = _file_metadata({
        "md5sum": "abc123",
        "datetime": "2026-01-15T12:00:00Z",
        "doctype": "io.cozy.files",
        "callback_token": "cozy-secret-token",
        "rag": {"api_key": "secret"},
    })
    assert result == {"doc_rev": "", "datetime": "2026-01-15T12:00:00Z", "doctype": "io.cozy.files"}


# ------------------------
# BUGF-03: Exit code on connection failure
# ------------------------
@pytest.mark.asyncio
async def test_main_exits_with_nonzero_on_connection_failure(monkeypatch):
    async def fake_connect_robust(url):
        raise AMQPConnectionError("Connection refused")

    monkeypatch.setattr("aio_pika.connect_robust", fake_connect_robust)

    with pytest.raises(SystemExit) as exc_info:
        await main()
    assert exc_info.value.code == 1


# ------------------------
# callback_token: signs the DLQ failure callback
# ------------------------
class _FakeExchange:
    def __init__(self):
        self.published = []

    async def publish(self, message, routing_key=None):
        self.published.append((message, routing_key))


class _FakeChannel:
    def __init__(self):
        self.default_exchange = _FakeExchange()


class _FakeCallbackResp:
    def __init__(self, status: int = 200):
        self.status = status

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class _FakeCallbackSession:
    """Fake ClientSession recording the failure-callback POST."""

    def __init__(self, status: int = 200):
        self._status = status
        self.post_call = None

    def post(self, url, json=None, headers=None, timeout=None):
        self.post_call = {"url": url, "json": json, "headers": headers}
        return _FakeCallbackResp(self._status)


class _DlqMessage:
    def __init__(self, body: bytes = b"{}", headers: dict | None = None):
        self.body = body
        self.headers = headers or {}
        self.content_type = "application/json"


@pytest.mark.asyncio
async def test_publish_to_dlq_signs_callback_with_bearer_token():
    """Sent as an Authorization bearer header, not in the URL."""
    channel, session = _FakeChannel(), _FakeCallbackSession()
    metadata = {
        "partition": "p1",
        "file_id": "f1",
        "doc_rev": "3-abc",
        "callback_url": "https://cozy.example/ai/index/status",
        "callback_token": "cozy-secret-token",
    }

    await publish_to_dlq(channel, _DlqMessage(), session, metadata=metadata)
    await _await_dlq_callbacks()

    call = session.post_call
    assert call["headers"] == {"Authorization": "Bearer cozy-secret-token"}
    assert call["url"] == "https://cozy.example/ai/index/status"
    # Never in a query string, never in the JSON body.
    assert "cozy-secret-token" not in call["url"]
    assert "cozy-secret-token" not in str(call["json"])
    assert len(channel.default_exchange.published) == 1


@pytest.mark.asyncio
async def test_publish_to_dlq_timestamp_matches_openrag_spelling():
    """Both callback producers must write the same timestamp: cozy-stack documents
    milliseconds and a "Z" suffix, not "+00:00" and microseconds.
    """
    channel, session = _FakeChannel(), _FakeCallbackSession()
    metadata = {
        "partition": "p1",
        "file_id": "f1",
        "doc_rev": "3-abc",
        "callback_url": "https://cozy.example/ai/index/status",
    }

    await publish_to_dlq(channel, _DlqMessage(), session, metadata=metadata)
    await _await_dlq_callbacks()

    timestamp = session.post_call["json"]["timestamp"]
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", timestamp), timestamp


@pytest.mark.asyncio
async def test_publish_to_dlq_without_token_sends_no_auth_header():
    """Optional field: an untokenised message works as before."""
    channel, session = _FakeChannel(), _FakeCallbackSession()
    metadata = {
        "partition": "p1",
        "file_id": "f1",
        "doc_rev": "3-abc",
        "callback_url": "https://cozy.example/ai/index/status",
    }

    await publish_to_dlq(channel, _DlqMessage(), session, metadata=metadata)
    await _await_dlq_callbacks()

    assert session.post_call["headers"] is None
    assert session.post_call["json"]["status"] == "error"


@pytest.mark.asyncio
async def test_publish_to_dlq_reads_callback_token_from_body():
    """The token is read from the message body when no metadata is passed."""
    channel, session = _FakeChannel(), _FakeCallbackSession()
    body = json.dumps({
        "partition": "p1",
        "file_id": "f1",
        "doc_rev": "3-abc",
        "callback_url": "https://cozy.example/ai/index/status",
        "callback_token": "tok-from-body",
    }).encode()

    # metadata=None -> publish_to_dlq extracts it itself
    await publish_to_dlq(channel, _DlqMessage(body=body), session, metadata=None)
    await _await_dlq_callbacks()

    assert session.post_call["headers"] == {"Authorization": "Bearer tok-from-body"}


@pytest.mark.asyncio
async def test_publish_to_dlq_skips_callback_without_doc_rev():
    """cozy-stack would answer 400: the message still lands in the DLQ, no POST."""
    channel, session = _FakeChannel(), _FakeCallbackSession()
    metadata = {"partition": "p1", "file_id": "f1", "callback_url": "https://cozy.example/cb"}

    await publish_to_dlq(channel, _DlqMessage(), session, metadata=metadata)
    await _await_dlq_callbacks()

    assert session.post_call is None
    assert len(channel.default_exchange.published) == 1


@pytest.mark.asyncio
async def test_drain_status_callbacks_waits_for_pending_posts():
    """A shutdown must not drop a status callback still in flight."""
    posted = []

    class _SlowResp(_FakeCallbackResp):
        async def __aenter__(self):
            await asyncio.sleep(0.05)
            posted.append(True)
            return self

    class _SlowSession(_FakeCallbackSession):
        def post(self, url, json=None, headers=None, timeout=None):
            return _SlowResp()

    metadata = {"partition": "p1", "file_id": "f1", "doc_rev": "3-abc", "callback_url": "https://cozy.example/cb"}
    transport.send_status_callback(_SlowSession(), metadata, "error")
    assert posted == []

    await transport.drain_status_callbacks()
    assert posted == [True]
