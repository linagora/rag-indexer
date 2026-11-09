import json

import pytest


class FakeResp:
    """Mimics RagResponse returned by rag_get_file / rag_delete."""
    def __init__(self, status: int, json_data=None, text_data=""):
        self.status = status
        self.json_data = json_data
        self.text = text_data


class FakeContextResp:
    """Async context manager that mimics aiohttp response for get_producer_file tests."""
    def __init__(self, status: int, body: bytes):
        self.status = status
        self._body = body
        self.content = self

    async def iter_chunked(self, size):
        for i in range(0, len(self._body), size):
            yield self._body[i:i + size]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class FakeSession:
    """Fake aiohttp.ClientSession whose .get() returns a FakeContextResp.

    IMPORTANT: .get() must be a regular (synchronous) method, NOT async.
    aiohttp's session.get() returns a context manager object synchronously;
    the async part is the `async with` that calls __aenter__ on it.
    """
    def __init__(self, resp: FakeContextResp):
        self._resp = resp

    def get(self, url, headers=None, timeout=None):
        # Returns the FakeContextResp directly -- it implements __aenter__/__aexit__
        return self._resp


class DummyMessage:
    """Enough for process_message(): the fields JSON-encoded in .body, AMQP .headers."""

    def __init__(self, fields: dict | None = None, headers: dict | None = None):
        self.body = json.dumps(fields).encode() if fields is not None else b""
        self.headers = headers or {}


@pytest.fixture
def headers_base():
    return {
        "partition": "user-1",
        "file_id": "file-123",
        "rag_base_url": "http://rag:8000",
        "rag_api_key": "secret",
        "content_type": "application/octet-stream",
        "name": "doc.txt",
        "md5sum": "d41d8cd98f00b204e9800998ecf8427e",
        "file_url": "http://cozy/files/downloads/sekret/doc.txt",
    }


@pytest.fixture
def aiohttp_session_stub():
    """Le process_message recoit un session aiohttp, mais comme on monkeypatch
    rag_get_file / rag_upsert / rag_delete, cette session ne sera pas reellement utilisee.
    """

    class _S:
        pass

    return _S()
