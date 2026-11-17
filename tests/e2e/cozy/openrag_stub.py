"""In-memory openRAG on 127.0.0.1:<port>, for running scenarios.py where no openRAG is
at hand (CI). It answers the routes the stack, the indexer and lib.py call; its search
is a substring match on the uploaded bytes (unzipped for an office file), so it tells
that the right content was uploaded, not that openRAG could parse it.

Usage: python3 -I openrag_stub.py <port> <API key>
"""
import io
import json
import re
import sys
import threading
import urllib.parse
import zipfile
from email.parser import BytesParser
from email.policy import HTTP
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(sys.argv[1])
KEY = sys.argv[2]
EXTENSIONS = ["txt", "md", "pdf", "docx", "odt", "pptx", "xlsx", "csv", "html"]

lock = threading.Lock()
# partition -> {"files": {file_id: {"metadata", "text", "workspaces"}}, "workspaces": {id: display_name}}
partitions = {}


def partition(p):
    return partitions.setdefault(p, {"files": {}, "workspaces": {}})


def searchable(content):
    if content[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            content = b" ".join(z.read(n) for n in z.namelist())
    return content.decode(errors="ignore")


def multipart(content_type, body):
    msg = BytesParser(policy=HTTP).parsebytes(b"Content-Type: " + content_type.encode() + b"\r\n\r\n" + body)
    return {part.get_param("name", header="content-disposition"): part for part in msg.iter_parts()}


ROUTES = []


def route(method, pattern):
    def register(fn):
        ROUTES.append((method, re.compile(pattern + "$"), fn))
        return fn
    return register


@route("GET", "/indexer/supported/types")
def supported_types(req):
    return 200, {"extensions": [f".{e}" for e in EXTENSIONS]}


@route("POST", "/partition/([^/]+)")
def create_partition(req, p):
    created = p not in partitions
    partition(p)
    return (201, {}) if created else (409, {"detail": "Partition already exists"})


@route("DELETE", "/partition/([^/]+)")
def delete_partition(req, p):
    return (204, None) if partitions.pop(p, None) else (404, {"detail": "Partition not found"})


@route("GET", "/partition/([^/]+)/")
def list_files(req, p):
    if p not in partitions:
        return 404, {"detail": "Partition not found"}
    return 200, {"files": [{"link": f"/partition/{p}/file/{f}"} for f in partitions[p]["files"]]}


@route("GET", "/partition/([^/]+)/workspaces")
def list_workspaces(req, p):
    if p not in partitions:
        return 404, {"detail": "Partition not found"}
    return 200, {"workspaces": [{"workspace_id": w, "display_name": n} for w, n in partitions[p]["workspaces"].items()]}


@route("POST", "/partition/([^/]+)/workspaces")
def create_workspace(req, p):
    body = req.json()
    ws = partition(p)["workspaces"]
    if body["workspace_id"] in ws:
        return 409, {"detail": "Workspace already exists"}
    ws[body["workspace_id"]] = body.get("display_name", "")
    return 201, {}


@route("GET", "/partition/([^/]+)/workspaces/([^/]+)")
def get_workspace(req, p, w):
    if w not in partitions.get(p, {}).get("workspaces", {}):
        return 404, {"detail": "Workspace not found"}
    return 200, {"workspace_id": w, "display_name": partitions[p]["workspaces"][w]}


@route("DELETE", "/partition/([^/]+)/workspaces/([^/]+)")
def delete_workspace(req, p, w):
    if partitions.get(p, {}).get("workspaces", {}).pop(w, None) is None:
        return 404, {"detail": "Workspace not found"}
    for f in partitions[p]["files"].values():
        f["workspaces"].discard(w)
    return 204, None


@route("POST", "/partition/([^/]+)/workspaces/([^/]+)/files")
def add_files(req, p, w):
    part = partitions.get(p)
    if not part or w not in part["workspaces"]:
        return 404, {"detail": "Workspace not found"}
    ids = req.json()["file_ids"]
    if any(f not in part["files"] for f in ids):
        return 404, {"detail": "File not found"}
    for f in ids:
        part["files"][f]["workspaces"].add(w)
    return 200, {}


@route("DELETE", "/partition/([^/]+)/workspaces/([^/]+)/files/([^/]+)")
def remove_file(req, p, w, f):
    doc = partitions.get(p, {}).get("files", {}).get(f)
    if not doc or w not in doc["workspaces"]:
        return 404, {"detail": "Not in the workspace"}
    doc["workspaces"].discard(w)
    return 204, None


@route("GET", "/partition/([^/]+)/files/([^/]+)/workspaces")
def file_workspaces(req, p, f):
    doc = partitions.get(p, {}).get("files", {}).get(f)
    if not doc:
        return 404, {"detail": "File not found"}
    return 200, {"workspace_ids": sorted(doc["workspaces"])}


@route("GET", "/partition/([^/]+)/file/([^/]+)")
def get_file(req, p, f):
    doc = partitions.get(p, {}).get("files", {}).get(f)
    return (200, {"metadata": doc["metadata"]}) if doc else (404, {"detail": "File not found"})


@route("POST|PUT", "/indexer/partition/([^/]+)/file/([^/]+)")
def upload(req, p, f):
    method = req.command
    files = partition(p)["files"]
    if method == "POST" and f in files:
        return 409, {"detail": f"File '{f}' already exists in partition {p}"}
    if method == "PUT" and f not in files:
        return 404, {"detail": "File not found"}
    parts = multipart(req.headers["Content-Type"], req.body)
    workspaces = set(json.loads(parts["workspace_ids"].get_content())) if "workspace_ids" in parts else set()
    if workspaces - partitions[p]["workspaces"].keys():
        return 404, {"detail": "Workspace not found"}
    meta = json.loads(parts["metadata"].get_content()) if "metadata" in parts else {}
    file = parts["file"]
    meta.update(file_id=f, partition=p, filename=file.get_filename())
    old = files.get(f, {}).get("workspaces", set())
    files[f] = {"metadata": meta, "text": searchable(file.get_payload(decode=True)), "workspaces": old | workspaces}
    return (201 if method == "POST" else 202), {"task_status_url": f"/indexer/task/{p}-{f}"}


@route("DELETE", "/indexer/partition/([^/]+)/file/([^/]+)")
def delete_file(req, p, f):
    return (204, None) if partitions.get(p, {}).get("files", {}).pop(f, None) else (404, {"detail": "File not found"})


@route("GET", "/search/partition/([^/]+)")
def search(req, p):
    q = req.query.get("text", [""])[0]
    top_k = int(req.query.get("top_k", ["10"])[0])
    hits = [doc for doc in partitions.get(p, {}).get("files", {}).values() if q and q in doc["text"]]
    return 200, {"documents": [{"metadata": doc["metadata"]} for doc in hits[:top_k]]}


class Handler(BaseHTTPRequestHandler):
    def _handle(self):
        url = urllib.parse.urlsplit(self.path)
        self.query = urllib.parse.parse_qs(url.query)
        self.body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if self.headers.get("Authorization") != f"Bearer {KEY}":
            return self._reply(401, {"detail": "Invalid API key"})
        for method, pattern, fn in ROUTES:
            m = pattern.match(url.path)
            if m and re.fullmatch(method, self.command):
                # Matched escaped, so that an id holding an escaped slash stays one segment.
                args = [urllib.parse.unquote(g) for g in m.groups()]
                try:
                    with lock:
                        result = fn(self, *args)
                except Exception as e:
                    # Reply rather than drop the connection, so the caller's log shows the stub's error.
                    self.log_error("%s: %r", fn.__name__, e)
                    result = 500, {"detail": f"stub: {e!r}"}
                return self._reply(*result)
        self._reply(404, {"detail": "No such route in the stub"})


    def json(self):
        return json.loads(self.body)

    def _reply(self, code, body):
        content = b"" if body is None else json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    do_GET = do_POST = do_PUT = do_DELETE = _handle


ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
