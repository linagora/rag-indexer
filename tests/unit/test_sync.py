"""The sync action against an in-process fake of cozy-stack and openRAG."""

import json
from dataclasses import dataclass, field

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from rag_indexer.errors import FatalError, TransientError
from rag_indexer.processing import process_message
from tests.conftest import DummyMessage


@dataclass
class StackFile:
    content: bytes
    md5sum: str = "md5-1"
    workspace_ids: list[str] = field(default_factory=lambda: ["ws1"])


@dataclass
class RagFile:
    content: bytes
    metadata: dict
    workspaces: set[str] = field(default_factory=set)


@dataclass
class World:
    stack: dict[str, StackFile] = field(default_factory=dict)
    rag: dict[str, RagFile] = field(default_factory=dict)
    workspaces: set[str] = field(default_factory=lambda: {"ws1", "ws2", "old"})
    calls: list[tuple[str, str]] = field(default_factory=list)

    def uploads(self) -> list[str]:
        return [m for m, p in self.calls if p.startswith("/indexer/") and m in ("POST", "PUT")]


def make_app(world: World) -> web.Application:
    def stack_file(request):
        file_id = request.match_info["id"]
        if request.headers.get("Authorization") != f"Bearer tok-{file_id}":
            raise web.HTTPUnauthorized()
        if file_id not in world.stack:
            raise web.HTTPNotFound()
        return world.stack[file_id]

    async def state(request):
        f = stack_file(request)
        return web.json_response({
            "name": "doc.txt",
            "md5sum": f.md5sum,
            "workspace_ids": f.workspace_ids,
            "metadata": {"md5sum": f.md5sum, "doctype": "io.cozy.files", "doc_rev": "2-b", "created_at": "c"},
        })

    async def content(request):
        return web.Response(body=stack_file(request).content)

    async def rag_get(request):
        f = world.rag.get(request.match_info["id"])
        if f is None:
            raise web.HTTPNotFound()
        return web.json_response({"metadata": f.metadata})

    async def rag_upload(request):
        file_id = request.match_info["id"]
        fields = {}
        async for part in await request.multipart():
            fields[part.name] = await part.read()
        workspace_ids = json.loads(fields.get("workspace_ids", b"[]"))
        if any(ws not in world.workspaces for ws in workspace_ids):
            raise web.HTTPNotFound()
        metadata = json.loads(fields["metadata"])
        if request.method == "POST":
            world.rag[file_id] = RagFile(fields["file"], metadata, set(workspace_ids))
        else:
            # A PUT replaces the content, the memberships are synced apart.
            world.rag[file_id].content = fields["file"]
            world.rag[file_id].metadata = metadata
        return web.json_response({}, status=201)

    async def rag_delete(request):
        if world.rag.pop(request.match_info["id"], None) is None:
            raise web.HTTPNotFound()
        return web.Response(status=204)

    async def file_workspaces(request):
        f = world.rag.get(request.match_info["id"])
        if f is None:
            raise web.HTTPNotFound()
        return web.json_response({"workspace_ids": sorted(f.workspaces)})

    async def add_to_workspace(request):
        for file_id in (await request.json())["file_ids"]:
            world.rag[file_id].workspaces.add(request.match_info["ws"])
        return web.json_response({}, status=201)

    async def remove_from_workspace(request):
        world.rag[request.match_info["id"]].workspaces.discard(request.match_info["ws"])
        return web.Response(status=204)

    @web.middleware
    async def record(request, handler):
        world.calls.append((request.method, request.path))
        return await handler(request)

    app = web.Application(middlewares=[record])
    r = app.router
    r.add_get("/ai/index/files/{id}", state)
    r.add_get("/ai/index/files/{id}/content", content)
    r.add_get("/partition/{p}/file/{id}", rag_get)
    r.add_post("/indexer/partition/{p}/file/{id}", rag_upload)
    r.add_put("/indexer/partition/{p}/file/{id}", rag_upload)
    r.add_delete("/indexer/partition/{p}/file/{id}", rag_delete)
    r.add_get("/partition/{p}/files/{id}/workspaces", file_workspaces)
    r.add_post("/partition/{p}/workspaces/{ws}/files", add_to_workspace)
    r.add_delete("/partition/{p}/workspaces/{ws}/files/{id}", remove_from_workspace)
    return app


@pytest.fixture
async def world():
    w = World()
    server = TestServer(make_app(w))
    await server.start_server()
    base = str(server.make_url("")).rstrip("/")
    async with aiohttp.ClientSession() as session:
        yield w, base, session
    await server.close()


async def sync(world, file_id="f1", **overrides):
    _, base, session = world
    fields = {
        "action": "sync",
        "partition": "alice.cozy",
        "file_id": file_id,
        "doc_rev": "1-a",
        "state_url": f"{base}/ai/index/files/{file_id}",
        "file_token": f"tok-{file_id}",
        "rag_base_url": base,
        "rag_api_key": "rag-key",
    } | overrides
    await process_message(DummyMessage(fields), session)


async def test_file_gone_on_the_stack_is_deleted(world):
    w, _, _ = world
    w.rag["f1"] = RagFile(b"x", {"md5sum": "md5-1"})
    await sync(world)
    assert "f1" not in w.rag


async def test_file_gone_everywhere_is_fine(world):
    await sync(world)


async def test_new_file_is_posted_with_its_workspaces(world):
    w, _, _ = world
    w.stack["f1"] = StackFile(b"hello", workspace_ids=["ws1", "ws2"])
    await sync(world)
    f = w.rag["f1"]
    assert f.content == b"hello"
    assert f.workspaces == {"ws1", "ws2"}
    assert f.metadata == {"md5sum": "md5-1", "datetime": "", "doctype": "io.cozy.files",
                          "created_at": "c", "doc_rev": "2-b"}
    assert w.uploads() == ["POST"]


async def test_up_to_date_file_only_syncs_its_workspaces(world):
    w, _, _ = world
    w.stack["f1"] = StackFile(b"hello", workspace_ids=["ws1"])
    w.rag["f1"] = RagFile(b"hello", {"md5sum": "md5-1"}, {"old"})
    await sync(world)
    assert w.rag["f1"].workspaces == {"ws1"}
    assert w.uploads() == []


async def test_changed_file_is_put_then_its_workspaces_synced(world):
    w, _, _ = world
    w.stack["f1"] = StackFile(b"new", md5sum="md5-2", workspace_ids=["ws1"])
    w.rag["f1"] = RagFile(b"old", {"md5sum": "md5-1"}, {"old"})
    await sync(world)
    assert w.rag["f1"].content == b"new"
    assert w.rag["f1"].workspaces == {"ws1"}
    assert w.uploads() == ["PUT"]


async def test_workspace_not_created_yet_is_retried(world):
    w, _, _ = world
    w.stack["f1"] = StackFile(b"hello", workspace_ids=["new-folder"])
    with pytest.raises(TransientError):
        await sync(world)


async def test_bad_token_is_fatal(world):
    w, _, _ = world
    w.stack["f1"] = StackFile(b"hello")
    with pytest.raises(FatalError):
        await sync(world, file_token="tok-other")


async def test_sync_without_token_is_invalid(world):
    with pytest.raises(FatalError, match="file_token"):
        await sync(world, file_token=None)
