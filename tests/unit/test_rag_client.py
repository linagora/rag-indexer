import io
import json

import aiohttp
import pytest

from rag_indexer.models import IndexMessage, RagConn, ContentSpec
from rag_indexer.rag_client import (
    get_producer_file,
    build_metadata,
    callback_metadata,
    metadata_dict,
    rag_upsert,
)
from rag_indexer.errors import FatalError, TransientError, UnsupportedError
from tests.conftest import FakeContextResp, FakeSession


def _upsert_msg(**kwargs) -> IndexMessage:
    """An upsert message with every required field, overridable."""
    fields = {
        "action": "upsert",
        "partition": "p1",
        "file_id": "f1",
        "name": "doc.txt",
        "md5sum": "d41d8cd98f00b204e9800998ecf8427e",
        "rag": RagConn(base_url="http://rag:8000", api_key="rag-key"),
        "content": ContentSpec(file_url="http://cozy/files/downloads/sekret/doc.txt"),
    }
    return IndexMessage(**{**fields, **kwargs})


# ------------------------
# BUGF-01: File download reads inside context manager
# ------------------------
@pytest.mark.asyncio
async def test_get_producer_file_reads_inside_context():
    expected_bytes = b"file-content-here"
    fake_resp = FakeContextResp(status=200, body=expected_bytes)
    fake_session = FakeSession(fake_resp)

    msg = _upsert_msg(
        content=ContentSpec(file_url="http://example.com/file.bin"),
    )
    dest = io.BytesIO()
    await get_producer_file(fake_session, msg, dest)
    assert dest.getvalue() == expected_bytes


@pytest.mark.asyncio
async def test_get_producer_file_invalid_url_is_fatal_without_the_secret():
    class _InvalidUrlSession:
        def get(self, url, **kwargs):
            raise aiohttp.InvalidURL(url)

    with pytest.raises(FatalError) as exc:
        await get_producer_file(_InvalidUrlSession(), _upsert_msg(), io.BytesIO())
    assert "sekret" not in str(exc.value)


@pytest.mark.asyncio
async def test_get_producer_file_disk_full_is_transient():
    class _FullDisk(io.BytesIO):
        def write(self, data):
            raise OSError(28, "No space left on device")

    session = FakeSession(FakeContextResp(status=200, body=b"x"))
    with pytest.raises(TransientError):
        await get_producer_file(session, _upsert_msg(), _FullDisk())


# ------------------------
# BUGF-02: Metadata merge
# ------------------------
def test_build_metadata_merges_app_metadata():
    msg = _upsert_msg(
        doc_rev="3-abc",
        doctype="pdf",
        app_metadata={"custom_key": "custom_value", "author": "test"},
    )
    meta = build_metadata(msg)
    assert meta["custom_key"] == "custom_value"
    assert meta["author"] == "test"
    assert meta["doc_rev"] == "3-abc"
    assert meta["doctype"] == "pdf"


def test_build_metadata_without_app_metadata():
    msg = _upsert_msg(
        doc_rev="3-abc",
        doctype="pdf",
    )
    meta = build_metadata(msg)
    assert meta["doc_rev"] == "3-abc"
    assert meta["doctype"] == "pdf"
    # No extra keys beyond the base fields
    assert "custom_key" not in meta


def test_build_metadata_exposes_md5sum_as_its_own_key():
    """cozy-stack's indexedMD5Sum reads metadata["md5sum"] on the existence GET."""
    msg = _upsert_msg(
        doc_rev="3-abc",
        md5sum="d41d8cd98f00b204e9800998ecf8427e",
    )
    meta = build_metadata(msg)
    assert meta["md5sum"] == "d41d8cd98f00b204e9800998ecf8427e"
    assert meta["doc_rev"] == "3-abc"


def test_build_metadata_never_substitutes_md5sum_for_doc_rev():
    """A md5sum has generation 0 for cozy's revision.Generation: every later
    status would be silently dropped as outdated. Empty is the only safe value.
    """
    msg = _upsert_msg(
        md5sum="d41d8cd98f00b204e9800998ecf8427e",
    )
    meta = build_metadata(msg)
    assert meta["doc_rev"] == ""
    assert meta["md5sum"] == "d41d8cd98f00b204e9800998ecf8427e"


# ------------------------
# NEW: File download via self-authenticating file_url
# ------------------------
@pytest.mark.asyncio
async def test_get_producer_file_no_auth_header():
    """get_producer_file downloads from file_url without any Authorization header.

    The file_url is self-authenticating (secret in the path), so no bearer is
    ever sent. This verifies the function returns content correctly.
    """
    expected_bytes = b"secure-file-content"
    fake_resp = FakeContextResp(status=200, body=expected_bytes)
    fake_session = FakeSession(fake_resp)

    msg = _upsert_msg(
        content=ContentSpec(
            file_url="http://example.com/files/downloads/sekret/secure.bin",
        ),
    )
    dest = io.BytesIO()
    await get_producer_file(fake_session, msg, dest)
    assert dest.getvalue() == expected_bytes


# ------------------------
# NEW: Metadata edge cases (TEST-03, TEST-04)
# ------------------------
def test_build_metadata_app_metadata_overrides_base_keys():
    """app_metadata still wins on the descriptive fields."""
    msg = _upsert_msg(
        doc_rev="3-abc",
        doctype="pdf",
        app_metadata={"doctype": "custom_doctype"},
    )
    meta = build_metadata(msg)
    assert meta["doctype"] == "custom_doctype"


def test_build_metadata_app_metadata_cannot_override_doc_rev():
    """Callbacks are ordered on doc_rev: application metadata must not displace it."""
    msg = _upsert_msg(
        doc_rev="3-abc",
        app_metadata={"doc_rev": "9-forged"},
    )
    meta = build_metadata(msg)
    assert meta["doc_rev"] == "3-abc"


def test_build_metadata_empty_dict_app_metadata():
    """app_metadata as empty dict should produce same result as no app_metadata."""
    msg = _upsert_msg(
        doc_rev="3-abc",
        doctype="pdf",
        app_metadata={},
    )
    meta = build_metadata(msg)
    assert meta["doc_rev"] == "3-abc"
    assert meta["doctype"] == "pdf"
    assert meta["datetime"] == ""
    assert set(meta) == {"doc_rev", "md5sum", "datetime", "doctype"}


def test_build_metadata_none_values_in_app_metadata():
    """app_metadata with None values should preserve them in the merge."""
    msg = _upsert_msg(
        doc_rev="3-abc",
        app_metadata={"extra": None},
    )
    meta = build_metadata(msg)
    assert meta["extra"] is None
    assert meta["doc_rev"] == "3-abc"


def test_build_metadata_no_doc_rev_no_md5sum():
    """Neither field set -- both fall back to the empty string, never to each other."""
    meta = metadata_dict(doc_rev=None, md5sum=None, datetime=None, doctype=None, app_metadata=None)
    assert meta["doc_rev"] == ""
    assert meta["md5sum"] == ""


def test_callback_metadata_narrows_to_what_cozy_reads():
    """callback_metadata() is our own narrowing for the DLQ failure callback, down
    to what cozy's handler reads (doc_rev) plus datetime/doctype. It is not what
    OpenRAG does: OpenRAG's real success callback echoes every metadata key except
    its own server-computed ones, so it also carries md5sum and app_metadata.
    """
    upsert_meta = build_metadata(
        _upsert_msg(
            doc_rev="3-abc",
            md5sum="d41d8cd98f00b204e9800998ecf8427e",
            datetime="2026-01-15T12:00:00Z",
            doctype="io.cozy.files",
            app_metadata={"custom": "value"},
        )
    )
    assert callback_metadata(upsert_meta) == {
        "doc_rev": "3-abc",
        "datetime": "2026-01-15T12:00:00Z",
        "doctype": "io.cozy.files",
    }


# ------------------------
# callback_token: multipart field to OpenRAG, never in the URL
# ------------------------
class _CapturingSession:
    """Fake ClientSession recording the rag_upsert request."""

    def __init__(self, status: int = 201, text: str = ""):
        self._status = status
        self._text = text
        self.request_call = None

    def get(self, url, headers=None, timeout=None):
        return FakeContextResp(status=200, body=b"file-bytes")

    def request(self, method, url, *, data=None, headers=None, timeout=None):
        self.request_call = {
            "method": method,
            "url": url,
            "form": {
                opts["name"]: value
                for opts, _hdrs, value in data._fields
                if "filename" not in opts
            },
            "headers": headers,
        }
        return _FakeRequestResp(self._status, self._text)


class _FakeRequestResp:
    def __init__(self, status: int, text: str = ""):
        self.status = status
        self._text = text

    async def text(self):
        return self._text

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


@pytest.mark.asyncio
async def test_rag_upsert_forwards_callback_token_as_form_field():
    session = _CapturingSession()
    msg = _upsert_msg(
        callback_url="https://cozy.example/ai/index/status",
        callback_token="cozy-secret-token",
        doc_rev="3-abc",
    )
    await rag_upsert(session, msg, is_new=True)

    call = session.request_call
    assert call["form"]["callback_token"] == "cozy-secret-token"
    assert call["form"]["callback_url"] == "https://cozy.example/ai/index/status"


@pytest.mark.asyncio
async def test_rag_upsert_never_puts_callback_token_in_query_string():
    """The token must not land in the URL or an auth header."""
    session = _CapturingSession()
    msg = _upsert_msg(callback_token="cozy-secret-token")
    await rag_upsert(session, msg, is_new=True)

    call = session.request_call
    assert "cozy-secret-token" not in call["url"]
    # Authorization must stay the OpenRAG api_key
    assert call["headers"]["Authorization"] == "Bearer rag-key"
    assert "cozy-secret-token" not in str(call["headers"])


@pytest.mark.asyncio
async def test_rag_upsert_omits_callback_token_when_absent():
    """Optional field: no callback_token means no such form field."""
    session = _CapturingSession()
    await rag_upsert(session, _upsert_msg(callback_url="https://cozy.example/cb", doc_rev="3-abc"), is_new=True)

    call = session.request_call
    assert "callback_token" not in call["form"]
    assert call["form"]["callback_url"] == "https://cozy.example/cb"


# ------------------------
# 409: only a duplicate content is final
# ------------------------
@pytest.mark.asyncio
async def test_rag_upsert_409_duplicate_content_is_skipped():
    """Another file of the partition holds the content: nothing to retry."""
    body = '{"detail": "[DOCUMENT_CONTENT_EXISTS]: This document already exists in partition \'p1\'."}'
    session = _CapturingSession(status=409, text=body)
    await rag_upsert(session, _upsert_msg(), is_new=True)  # must not raise


@pytest.mark.asyncio
@pytest.mark.parametrize("detail", [
    "File 'f1' already exists in partition p1",
    "File 'f1' is already being indexed in partition 'p1'.",
])
async def test_rag_upsert_other_409_is_retried(detail):
    """A concurrent upload or an indexing still running: retry, then PUT if needed."""
    session = _CapturingSession(status=409, text=json.dumps({"detail": detail}))
    with pytest.raises(TransientError):
        await rag_upsert(session, _upsert_msg(), is_new=True)


@pytest.mark.asyncio
async def test_rag_upsert_415_is_unsupported():
    """openRAG refuses the format for good: reported as such, not as a failure."""
    session = _CapturingSession(status=415, text='{"detail": "Unsupported file format: xyz"}')
    with pytest.raises(UnsupportedError):
        await rag_upsert(session, _upsert_msg(name="doc.xyz"), is_new=True)
