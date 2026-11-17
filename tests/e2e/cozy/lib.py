"""Helpers of the cozy-stack e2e scenarios: the stack, openRAG and RabbitMQ, configured
by the environment (see env.example next to this file)."""
import base64
import json
import os
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid


def env(name, default=None):
    value = os.environ.get(name, default)
    if value is None:
        sys.exit(f"{name} is not set: see tests/e2e/cozy/env.example")
    return value


DOMAIN = env("E2E_DOMAIN")
SCHEME = env("E2E_STACK_SCHEME", "http")
STACK = f"{SCHEME}://{DOMAIN}"
COZY_STACK = env("COZY_STACK_BIN", "cozy-stack")
ADMIN_PORT = env("COZY_ADMIN_PORT", "6060")
RAG_URL = env("E2E_RAG_URL").rstrip("/")
RAG_KEY = env("E2E_RAG_API_KEY")
ILOG = env("E2E_INDEXER_LOG")
SLOG = env("E2E_STACK_LOG")
MGMT = env("E2E_RABBITMQ_MGMT_URL", "http://localhost:15672/api").rstrip("/")
MGMT_AUTH = {"Authorization": "Basic " + base64.b64encode(env("E2E_RABBITMQ_MGMT_AUTH", "guest:guest").encode()).decode()}


def cli(*args):
    return subprocess.run([COZY_STACK, *args, "--admin-port", ADMIN_PORT], check=True,
                          capture_output=True, text=True).stdout.strip()


TOKEN = cli("instances", "token-cli", DOMAIN,
            "io.cozy.files io.cozy.triggers io.cozy.jobs io.cozy.ai.chat.assistants io.cozy.ai.chat.rag")


def http(method, url, body=None, headers=None, raw=False):
    data = body
    headers = dict(headers or {})
    if isinstance(body, (dict, list)):
        data = json.dumps(body).encode()
        headers.setdefault("Content-Type", "application/json")
    elif isinstance(body, str):
        data = body.encode()
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            status, content = r.status, r.read()
    except urllib.error.HTTPError as e:
        status, content = e.code, e.read()
    except (urllib.error.URLError, ConnectionError) as e:
        return 0, str(e)
    if raw:
        return status, content
    try:
        return status, json.loads(content) if content else None
    except ValueError:
        return status, content.decode(errors="replace")


def stack(method, path, body=None, ctype=None):
    h = {"Authorization": f"Bearer {TOKEN}"}
    if ctype:
        h["Content-Type"] = ctype
    return http(method, STACK + path, body, h)


def fault(mode):
    """Set the mode of fault_proxy.py."""
    with open(env("E2E_FAULT_MODE_FILE"), "w") as f:
        f.write(mode + "\n")


def rag(method, path, body=None):
    return http(method, RAG_URL + path, body, {"Authorization": f"Bearer {RAG_KEY}"})


P = urllib.parse.quote(DOMAIN, safe="")


def ok(status, body, *expected):
    if status not in (expected or (200, 201)):
        raise AssertionError(f"HTTP {status}: {body}")
    return body


def admin(method, path):
    return http(method, f"http://localhost:{ADMIN_PORT}/instances/{DOMAIN}{path}")


def uniq(word="mot"):
    return f"{word}{uuid.uuid4().hex[:10]}"


def text(*tokens):
    return f"Document de test {uuid.uuid4()}.\n" + "\n".join(f"Le code secret est {t}." for t in tokens) + "\n"


# --- stack: files -------------------------------------------------------------

ROOT = "io.cozy.files.root-dir"


def mkdir(parent, name):
    q = urllib.parse.urlencode({"Type": "directory", "Name": name})
    return ok(*stack("POST", f"/files/{parent}?{q}"))["data"]["id"]


def upload(dir_id, name, content, mime="text/plain"):
    q = urllib.parse.urlencode({"Type": "file", "Name": name})
    data = content.encode() if isinstance(content, str) else content
    return ok(*stack("POST", f"/files/{dir_id}?{q}", data, ctype=mime))["data"]["id"]


def put_content(file_id, content, mime="text/plain"):
    data = content.encode() if isinstance(content, str) else content
    return ok(*stack("PUT", f"/files/{file_id}", data, ctype=mime))


def patch(file_id, **attrs):
    body = {"data": {"type": "io.cozy.files", "id": file_id, "attributes": attrs}}
    return ok(*stack("PATCH", f"/files/{file_id}", body, ctype="application/vnd.api+json"))


def trash(file_id):
    return ok(*stack("DELETE", f"/files/{file_id}"))


def restore(file_id):
    return ok(*stack("POST", f"/files/trash/{file_id}"))


def destroy(file_id):
    return ok(*stack("DELETE", f"/files/trash/{file_id}"), 204)


def file_doc(file_id):
    return ok(*stack("GET", f"/files/{file_id}"))["data"]


def md5_of(file_id):
    return base64.b64decode(file_doc(file_id)["attributes"]["md5sum"]).hex()


# --- stack: assistants, triggers, statuses --------------------------------------

def create_assistant(name, dir_ids):
    doc = {"name": name, "relationships": {"knowledgeBase": {
        "data": [{"_id": d, "_type": "io.cozy.files"} for d in dir_ids]}}}
    return ok(*stack("POST", "/data/io.cozy.ai.chat.assistants/", doc))["id"]


def delete_assistant(aid):
    doc = ok(*stack("GET", f"/data/io.cozy.ai.chat.assistants/{aid}"))
    return ok(*stack("DELETE", f"/data/io.cozy.ai.chat.assistants/{aid}?rev={doc['_rev']}"))


def ensure_triggers(debounce="5s"):
    """The two rag-index triggers of docs/ai.md, with a short debounce, unless the
    instance already has rag-index triggers."""
    _, res = stack("GET", "/jobs/triggers?Worker=rag-index")
    if res and res.get("data"):
        return
    for args, extra in (("io.cozy.files", {"debounce": debounce}), ("io.cozy.ai.chat.assistants", {})):
        body = {"data": {"attributes": {"type": "@event", "arguments": args, "worker": "rag-index",
                                        "message": {"doctype": "io.cozy.files"}, **extra}}}
        ok(*stack("POST", "/jobs/triggers", body))


def rag_status(file_id):
    s, body = stack("GET", f"/data/io.cozy.ai.chat.rag/{file_id}")
    return body if s == 200 else None


# --- openRAG ------------------------------------------------------------------

def rag_file(file_id):
    return rag("GET", f"/partition/{P}/file/{file_id}")


def rag_ws_of(file_id):
    s, body = rag("GET", f"/partition/{P}/files/{file_id}/workspaces")
    return sorted(body.get("workspace_ids") or []) if s == 200 else None


def rag_workspaces():
    s, body = rag("GET", f"/partition/{P}/workspaces")
    return body if s == 200 else None


def rag_md5(file_id):
    s, body = rag_file(file_id)
    return (body.get("metadata") or {}).get("md5sum") if s == 200 else None


def found(text, file_id, top_k=10):
    s, res = rag("GET", f"/search/partition/{P}?" + urllib.parse.urlencode({"text": text, "top_k": top_k}))
    docs = res.get("documents", []) if s == 200 else []
    return any((d.get("metadata") or {}).get("file_id") == file_id for d in docs)


def wait_ws(dir_id, present=True):
    def pred():
        ids = [w["workspace_id"] for w in (rag_workspaces() or {}).get("workspaces", [])]
        return (dir_id in ids) == present
    return wait(pred, what=f"workspace {dir_id} {'created' if present else 'deleted'}")


# --- waiting --------------------------------------------------------------------

def wait(pred, timeout=240, step=3, what=""):
    start = time.time()
    last = None
    while time.time() - start < timeout:
        last = pred()
        if last:
            return round(time.time() - start, 1)
        time.sleep(step)
    raise AssertionError(f"timeout {timeout}s waiting for {what} (last: {last!r})")


def indexed(file_id, md5=None, ws=None):
    """Predicate: openRAG holds the file, with this md5sum and these workspaces."""
    def pred():
        m = rag_md5(file_id)
        if m is None or (md5 and m != md5):
            return False
        return ws is None or rag_ws_of(file_id) == sorted(ws)
    return pred


def gone(file_id):
    return lambda: rag_file(file_id)[0] == 404


# --- logs -------------------------------------------------------------------------

def mark(path=ILOG):
    return os.path.getsize(path)


def events(since, file_id=None, name=None):
    """Indexer log events (JSON lines) written since the mark."""
    out = []
    with open(ILOG) as f:
        f.seek(since)
        for line in f:
            try:
                e = json.loads(line)
            except ValueError:
                continue
            if (file_id is None or e.get("file_id") == file_id) and (name is None or e.get("event") == name):
                out.append(e)
    return out


def uploads(since, file_id=None):
    return [e for e in events(since, file_id, "rag_api_call")
            if e.get("endpoint") == "indexer" and e.get("method") in ("POST", "PUT")]


def stack_log(since):
    with open(SLOG) as f:
        f.seek(since)
        return f.read()


def quiet(seconds=8, timeout=300):
    """Wait until neither the indexer nor the stack logged anything for that long."""
    start = time.time()
    last = (mark(), mark(SLOG))
    calm = time.time()
    while time.time() - start < timeout:
        time.sleep(1)
        now = (mark(), mark(SLOG))
        if now != last:
            last, calm = now, time.time()
        elif time.time() - calm >= seconds:
            return
    raise AssertionError("the indexer and the stack never went quiet")


# --- RabbitMQ (management API) -----------------------------------------------------

def queue_len(name):
    s, body = http("GET", f"{MGMT}/queues/%2F/{name}", headers=MGMT_AUTH)
    return body.get("messages", 0) if s == 200 else None


def peek(name, count=50):
    """The messages of a queue, put back in place."""
    s, body = http("POST", f"{MGMT}/queues/%2F/{name}/get",
                   {"count": count, "ackmode": "ack_requeue_true", "encoding": "auto"}, MGMT_AUTH)
    return [json.loads(m["payload"]) for m in body] if s == 200 else []


# --- runner -----------------------------------------------------------------------

SCENARIOS = []


def scenario(fn):
    SCENARIOS.append(fn)
    return fn


def main(setup):
    """Run the scenarios named on the command line (all by default). Each result is
    also appended, as a JSON line, to the file E2E_RESULTS names, if set."""
    sel = sys.argv[1:]
    ctx = {}
    setup(ctx)
    passed = failed = 0
    for fn in SCENARIOS:
        sid = fn.__name__.split("_")[0]
        if sel and sid not in sel and fn.__name__ not in sel:
            continue
        notes = []
        start = time.time()
        try:
            fn(ctx, notes.append)
            status = "PASS"
            passed += 1
        except Exception as e:
            status = "FAIL"
            failed += 1
            notes.append(f"{type(e).__name__}: {e}")
            if not isinstance(e, AssertionError):
                notes.append(traceback.format_exc(limit=3))
        dur = round(time.time() - start, 1)
        print(f"{status} {sid} {fn.__doc__.strip()} ({dur}s)", flush=True)
        for n in notes:
            print(f"     {n}", flush=True)
        if os.environ.get("E2E_RESULTS"):
            with open(os.environ["E2E_RESULTS"], "a") as f:
                f.write(json.dumps({"at": time.strftime("%FT%T"), "domain": DOMAIN, "id": sid,
                                    "title": fn.__doc__.strip(), "status": status, "duration_s": dur,
                                    "notes": notes}) + "\n")
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
