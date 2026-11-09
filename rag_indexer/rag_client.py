import asyncio
import json
import tempfile
from dataclasses import dataclass
from typing import Any

import aiohttp
import structlog
from aiohttp import FormData

from rag_indexer.config import HTTP_TIMEOUT
from rag_indexer.models import IndexMessage, RagConn
from rag_indexer.errors import TransientError, FatalError, UnsupportedError

log = structlog.get_logger()


@dataclass
class RagResponse:
    status: int
    text: str = ""
    json_data: dict[str, Any] | None = None


async def rag_get_file(
    session: aiohttp.ClientSession, rag: RagConn, partition: str, file_id: str
) -> RagResponse:
    log.debug("rag_api_call", method="GET", endpoint="file")
    url = f"{rag.base_url}/partition/{partition}/file/{file_id}"
    async with session.get(
        url, headers={"Authorization": f"Bearer {rag.api_key}"}, timeout=HTTP_TIMEOUT
    ) as resp:
        text = await resp.text()
        json_data = None
        if resp.status == 200:
            try:
                json_data = json.loads(text)
            except ValueError:
                pass
        return RagResponse(status=resp.status, text=text, json_data=json_data)


async def rag_delete(
    session: aiohttp.ClientSession, rag: RagConn, partition: str, file_id: str
) -> RagResponse:
    log.debug("rag_api_call", method="DELETE", endpoint="file")
    url = f"{rag.base_url}/indexer/partition/{partition}/file/{file_id}"
    async with session.delete(
        url, headers={"Authorization": f"Bearer {rag.api_key}"}, timeout=HTTP_TIMEOUT
    ) as resp:
        text = await resp.text()
        return RagResponse(status=resp.status, text=text)


_DOWNLOAD_CHUNK_SIZE = 64 * 1024


async def get_producer_file(session: aiohttp.ClientSession, msg: IndexMessage, dest) -> None:
    """Write the content at msg's file_url into the binary file object dest.

    The file_url carries its own authentication via the secret in its path
    (cozy-stack /files/downloads/<secret>/<filename>); no Authorization header.
    """
    log.debug("file_download", source="file_url")
    try:
        async with session.get(msg.content.file_url, timeout=HTTP_TIMEOUT) as resp:
            if resp.status == 429 or resp.status >= 500:
                raise TransientError(f"Failed to fetch file_url ({resp.status})")
            if resp.status >= 400:
                raise FatalError(f"Failed to fetch file_url ({resp.status})")
            async for chunk in resp.content.iter_chunked(_DOWNLOAD_CHUNK_SIZE):
                dest.write(chunk)
    except TransientError:
        raise
    except FatalError:
        raise
    except asyncio.TimeoutError as e:
        raise TransientError(f"Timeout fetching file_url: {e}") from e
    except aiohttp.InvalidURL as e:
        # Its message is the URL itself, secret included: never put it in the error.
        raise FatalError("Invalid file_url") from e
    except aiohttp.ClientError as e:
        raise TransientError(f"Network error fetching file_url: {e}") from e
    except OSError as e:
        # Writing the temporary file (disk full): the next attempt may succeed.
        raise TransientError(f"Cannot store the file_url download: {e}") from e


# Marker of the 409 openRAG answers when another file of the partition already
# holds the same content (cozy-stack's classifyConflict reads the same one).
_DUPLICATE_CONTENT_CODE = "DOCUMENT_CONTENT_EXISTS"


def build_metadata(msg: IndexMessage) -> dict[str, Any]:
    """Metadata stored on the OpenRAG document.

    doc_rev is never filled in from another field: cozy-stack reads it as a CouchDB
    revision, and anything else has generation 0 there, which makes every later status
    look outdated and be dropped without a trace.
    """
    meta = {
        "md5sum": msg.md5sum or "",
        "datetime": msg.datetime or "",
        "doctype": msg.doctype or "",
    }
    if isinstance(msg.app_metadata, dict):
        meta |= msg.app_metadata
    # Set last: the callbacks are ordered on this, application metadata must not move it.
    meta["doc_rev"] = msg.doc_rev or ""
    return meta


async def rag_upsert(
    session: aiohttp.ClientSession,
    msg: IndexMessage,
    is_new: bool
) -> None:
    # Through a file on disk: openRAG takes uploads up to 1 GB, too much to hold in
    # memory for each of the messages processed in parallel.
    with tempfile.TemporaryFile() as content:
        await get_producer_file(session, msg, content)
        content.seek(0)
        await _send_upsert(session, msg, is_new, content)


async def _send_upsert(
    session: aiohttp.ClientSession,
    msg: IndexMessage,
    is_new: bool,
    content,
) -> None:
    form = FormData()
    rag = msg.rag

    content_type = msg.content_type or "application/octet-stream"
    form.add_field("file", content, filename=msg.name, content_type=content_type)

    meta = build_metadata(msg)

    form.add_field("metadata", json.dumps(meta))

    # Forward the cozy callback URL so OpenRAG can POST the async indexing status.
    if msg.callback_url:
        form.add_field("callback_url", msg.callback_url)
        log.debug("upsert_callback_url_forwarded", callback_url=msg.callback_url)

    # Token for OpenRAG to sign the callback. Presence is logged, never the value.
    if msg.callback_token:
        form.add_field("callback_token", msg.callback_token)
        log.debug("upsert_callback_token_forwarded")

    url_base = f"{rag.base_url}/indexer/partition/{msg.partition}/file/{msg.file_id}"

    method = "POST" if is_new else "PUT"
    headers = {"Authorization": f"Bearer {msg.rag.api_key}"}

    log.debug("rag_api_call", method=method, endpoint="indexer")
    async with session.request(
        method,
        url_base,
        data=form,
        headers=headers,
        timeout=HTTP_TIMEOUT,
    ) as resp:
        # read response body to release the connection
        resp_text = await resp.text()
        if resp.status == 429 or resp.status >= 500:
            raise TransientError(f"RAG {method} {resp.status}: {resp_text}")
        if resp.status == 409:
            if _DUPLICATE_CONTENT_CODE in resp_text:
                # openRAG holds one document per content in a partition: this file id
                # will never be indexed, now or later.
                log.info("rag_upsert_duplicate_content", method=method)
                return
            # The id is already known (a concurrent upload) or still being indexed:
            # the retry reads it again and sends a PUT if its content differs.
            raise TransientError(f"RAG {method} 409: {resp_text}")
        if resp.status == 415:
            raise UnsupportedError(f"RAG {method} 415: {resp_text}")
        if resp.status >= 400:
            raise FatalError(f"RAG {method} {resp.status}: {resp_text}")
        return
