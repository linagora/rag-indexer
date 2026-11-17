"""Failure scenarios of the indexing of a cozy-stack instance through rag-indexer.

They run on the instance E2E_FAULTS_DOMAIN, whose openRAG server is the one of
E2E_RAG_URL behind fault_proxy.py (E2E_FAULTS_RAG_URL), and stop and start the
indexer and RabbitMQ with the commands of the environment (see README.md).

python3 -I tests/e2e/cozy/faults.py [F01 F02 ...]   (all by default)
"""
import json
import os
import subprocess
import sys
import time
import urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
# The other instance, the one of the functional scenarios.
OTHER_DOMAIN = os.environ.get("E2E_DOMAIN", "")
for name in ("E2E_DOMAIN", "E2E_FAULTS_DOMAIN", "E2E_FAULTS_RAG_URL"):
    if not os.environ.get(name):
        sys.exit(f"{name} is not set: see tests/e2e/cozy/env.example")
os.environ["E2E_DOMAIN"] = os.environ["E2E_FAULTS_DOMAIN"]
os.environ["E2E_RAG_URL"] = os.environ["E2E_FAULTS_RAG_URL"]
sys.path.insert(0, HERE)
from lib import *  # noqa: E402,F403
from lib import env  # noqa: E402

FAULT_MODE_FILE = env("E2E_FAULT_MODE_FILE")
INDEXER_HEALTH = env("E2E_INDEXER_HEALTH_URL", "http://localhost:8080/health")
RUN = time.strftime("%H%M%S")


def fault(mode):
    with open(FAULT_MODE_FILE, "w") as f:
        f.write(mode + "\n")


def run(command_env):
    subprocess.run(env(command_env), shell=True, check=True)


def indexer(up):
    if up:
        run("E2E_INDEXER_START")
        wait(lambda: http("GET", INDEXER_HEALTH)[0] == 200, timeout=60, step=1, what="indexer up")
    else:
        run("E2E_INDEXER_STOP")
        wait(lambda: http("GET", INDEXER_HEALTH)[0] == 0, timeout=30, step=1, what="indexer down")


def rabbit(up):
    run("E2E_RABBITMQ_START" if up else "E2E_RABBITMQ_STOP")
    if up:
        wait(lambda: http("GET", f"{MGMT}/overview", headers=MGMT_AUTH)[0] == 200, timeout=60, step=1,
             what="rabbitmq up")


def dlq_replay(file_id):
    r = subprocess.run(["uv", "run", "python", "scripts/dlq_replay.py", "replay", file_id], cwd=REPO,
                       env={**os.environ, "RABBITMQ_URL": env("E2E_RABBITMQ_URL")},
                       capture_output=True, text=True)
    if r.returncode:
        raise AssertionError(f"dlq_replay failed ({r.returncode}): {r.stderr.strip()[-300:]}")
    return r.stdout.strip()


def stack_errors(since):
    return [line[:260] for line in stack_log(since).splitlines()
            if ("level=error" in line or "level=warning" in line) and DOMAIN in line]


def queued_for(file_id, queue="rag.index.q"):
    return [m for m in peek(queue) if m.get("file_id") == file_id]


def peek_one_for(file_id):
    """A real message the stack publishes for the file (published while the indexer is stopped)."""
    indexer(False)
    try:
        put_content(file_id, text(uniq()))
        wait(lambda: queued_for(file_id), what="message queued")
        return queued_for(file_id)[0]
    finally:
        indexer(True)


def setup(ctx):
    fault("ok")
    indexer(True)
    ensure_triggers()
    ctx["kb"] = mkdir(ROOT, f"KB-{RUN}")
    ctx["out"] = mkdir(ROOT, f"OUT-{RUN}")
    create_assistant(f"faults {RUN}", [ctx["kb"]])
    wait_ws(ctx["kb"])
    print(f"run {RUN} on {DOMAIN}: kb {ctx['kb']}")


@scenario
def F01_indexer_down(ctx, note):
    """Indexer stopped: messages wait in rag.index.q (format checked, file routes guarded); restarted: indexed"""
    indexer(False)
    try:
        c1, c2 = text(uniq()), text(uniq())
        f1, f2 = upload(ctx["kb"], "attente1.txt", c1), upload(ctx["kb"], "attente2.txt", c2)
        wait(lambda: queued_for(f1) and queued_for(f2), what="messages queued")
        msg, other = queued_for(f1)[0], queued_for(f2)[0]
        note(f"queue length {queue_len('rag.index.q')}; message keys {sorted(msg)}")
        rev = file_doc(f1)["meta"]["rev"]
        assert msg["action"] == "sync" and msg["partition"] == DOMAIN and msg["file_id"] == f1
        assert msg["doc_rev"] == rev, (msg["doc_rev"], rev)
        assert msg["state_url"] == f"{STACK}/ai/index/files/{f1}"
        assert msg["callback_url"] == f"{STACK}/ai/index/status"
        assert msg["rag_base_url"] == RAG_URL and "rag_api_key" not in msg, "the broker must carry no openRAG key"

        def get(url, tok):
            return http("GET", url, headers={"Authorization": f"Bearer {tok}"} if tok else {}, raw=True)
        s, body = get(msg["state_url"], msg["file_token"])
        state = json.loads(body)
        assert s == 200 and state["workspace_ids"] == [ctx["kb"]] and state["metadata"]["doc_rev"] == rev, state
        assert state["md5sum"] == md5_of(f1)
        assert get(msg["state_url"] + "/content", msg["file_token"]) == (200, c1.encode())
        codes = {"no token": get(msg["state_url"], None)[0],
                 "token of another file": get(msg["state_url"], other["file_token"])[0],
                 "token of another file, content": get(msg["state_url"] + "/content", other["file_token"])[0],
                 "garbage token": get(msg["state_url"], "abc.def.ghi")[0],
                 "CLI token of the instance": get(msg["state_url"], TOKEN)[0],
                 "token on the other instance": get(f"{SCHEME}://{OTHER_DOMAIN}/ai/index/files/{f1}",
                                                     msg["file_token"])[0]}
        note(f"file routes: {codes}")
        assert all(c in (401, 403) for c in codes.values()), codes
    finally:
        indexer(True)
    for f in (f1, f2):
        wait(indexed(f, md5_of(f), [ctx["kb"]]), what=f"{f} indexed")
    wait(lambda: queue_len("rag.index.q") == 0, timeout=30, what="queue drained (management stats lag ~5s)")


@scenario
def F02_late_message(ctx, note):
    """Message of an upload processed after the file was trashed: nothing indexed (the state is read late)"""
    indexer(False)
    try:
        f = upload(ctx["kb"], "ephemere.txt", text(uniq()))
        wait(lambda: queued_for(f), what="upload message queued")
        trash(f)
        wait(lambda: len(queued_for(f)) >= 2, what="trash message queued")
        m = mark()
    finally:
        indexer(True)
    wait(lambda: len(events(m, f, "message_processed")) >= 2, what="both processed")
    gone_logs = len(events(m, f, "sync_file_gone"))
    assert not uploads(m, f), "uploaded"
    assert rag_file(f)[0] == 404
    note(f"2 messages, {gone_logs} read a 404 state, no upload")


@scenario
def F03_transient(ctx, note):
    """openRAG answers 503 to the indexer for a while: retried, then indexed"""
    fault("indexer:503")
    m = mark()
    f = upload(ctx["kb"], "retry.txt", text(uniq()))
    try:
        wait(lambda: events(m, f, "message_retry"), what="first retry")
    finally:
        fault("ok")
    t = wait(indexed(f, md5_of(f), [ctx["kb"]]), what="indexed")
    note(f"indexed {t}s after the fault was lifted; queues: {[e['queue'] for e in events(m, f, 'message_retry')]}")


@scenario
def F04_dlq_exhausted(ctx, note):
    """openRAG answers 503 past the last retry: DLQ, error status on the stack; DLQ replayed after the fix: indexed"""
    fault("indexer:503")
    m = mark()
    f = upload(ctx["kb"], "dlq.txt", text(uniq()))
    try:
        t = wait(lambda: queued_for(f, "rag.index.dlq"), timeout=180, what="message in the DLQ")
        wait(lambda: rag_status(f), timeout=30, what="error status")
        st = rag_status(f)
        note(f"DLQ after {t}s, {len(events(m, f, 'message_retry'))} retries; status {st['status']!r} "
             f"docRev {st.get('docRev')} (file rev {file_doc(f)['meta']['rev']})")
        assert st["status"] == "error" and st.get("docRev") == file_doc(f)["meta"]["rev"]
    finally:
        fault("ok")
    note(f"replay: {dlq_replay(f)[-120:]!r}")
    t = wait(indexed(f, md5_of(f), [ctx["kb"]]), what="indexed after the replay")
    note(f"indexed {t}s after the replay; status on the stack now {rag_status(f).get('status')!r}")


@scenario
def F05_fatal(ctx, note):
    """openRAG answers 403 to the indexer: DLQ at once (no retry), error status; replayed after the fix: indexed"""
    fault("indexer:403")
    m = mark()
    f = upload(ctx["kb"], "fatal.txt", text(uniq()))
    try:
        t = wait(lambda: queued_for(f, "rag.index.dlq"), timeout=60, what="message in the DLQ")
        wait(lambda: rag_status(f), timeout=30, what="error status")
        assert rag_status(f)["status"] == "error"
        assert not events(m, f, "message_retry"), "retried"
        note(f"DLQ after {t}s without retry")
    finally:
        fault("ok")
    dlq_replay(f)
    wait(indexed(f, md5_of(f), [ctx["kb"]]), what="indexed after the replay")


@scenario
def F06_openrag_down_for_stack(ctx, note):
    """openRAG down for the stack too: the job fails and keeps its checkpoint; openRAG back: indexed"""
    fault("all:503")
    ms, m = mark(SLOG), mark()
    f = upload(ctx["kb"], "panne.txt", text(uniq()))
    try:
        wait(lambda: stack_errors(ms), timeout=60, what="stack error")
        time.sleep(5)
        assert not events(m, f), "published while openRAG was down"
        note(f"stack: {stack_errors(ms)[0]}")
    finally:
        fault("ok")
    t = wait(indexed(f, md5_of(f), [ctx["kb"]]), timeout=180, what="indexed")
    note(f"indexed {t}s after openRAG came back, without any other change (job retry)")


@scenario
def F07_rabbitmq_down(ctx, note):
    """RabbitMQ stopped: publishing fails, checkpoint kept; RabbitMQ back: stack and indexer reconnect, indexed"""
    rabbit(False)
    ms = mark(SLOG)
    try:
        f = upload(ctx["kb"], "rabbit.txt", text(uniq()))
        wait(lambda: stack_errors(ms), timeout=60, what="stack error")
        note(f"stack: {stack_errors(ms)[0]}")
    finally:
        rabbit(True)
    try:
        t = wait(indexed(f, md5_of(f), [ctx["kb"]]), timeout=180, what="indexed without another change")
        note(f"indexed {t}s after RabbitMQ came back, without any other change")
    except AssertionError:
        g = upload(ctx["kb"], "rabbit-2.txt", text(uniq()))
        t = wait(indexed(f, md5_of(f), [ctx["kb"]]), timeout=180, what="indexed after another change")
        wait(indexed(g, md5_of(g)), what="the other change indexed")
        note(f"NOT indexed until another change of the instance; then after {t}s")
    s, h = http("GET", INDEXER_HEALTH)
    note(f"indexer health after the outage: {s} {h}")


@scenario
def F08_no_queue(ctx, note):
    """No queue bound to the exchange: the publication is an error, checkpoint kept; queue back: indexed"""
    indexer(False)
    ms = mark(SLOG)
    try:
        http("DELETE", f"{MGMT}/queues/%2F/rag.index.q", headers=MGMT_AUTH)
        f = upload(ctx["kb"], "orphelin.txt", text(uniq()))
        wait(lambda: stack_errors(ms), timeout=60, what="stack error")
        note(f"stack: {stack_errors(ms)[0]}")
    finally:
        indexer(True)
    t = wait(indexed(f, md5_of(f), [ctx["kb"]]), timeout=180, what="indexed")
    note(f"indexed {t}s after the queue was declared again, without any other change")


@scenario
def F09_isolation(ctx, note):
    """Partitions: a file of this instance is not on the partition of the other one"""
    f = upload(ctx["kb"], "isole.txt", text(uniq()))
    wait(indexed(f, md5_of(f)), what="indexed")
    other = urllib.parse.quote(OTHER_DOMAIN, safe="")
    assert rag("GET", f"/partition/{other}/file/{f}")[0] == 404


@scenario
def F10_long_rabbitmq_outage(ctx, note):
    """RabbitMQ stopped ~2 min (longer than the 3 job attempts): file indexed once it is back, the next change carries it anyway"""
    rabbit(False)
    ms = mark(SLOG)
    try:
        f = upload(ctx["kb"], "longue-panne.txt", text(uniq()))
        time.sleep(110)
        note(f"{sum('rabbitmq publish' in e for e in stack_errors(ms))} publish errors during the outage")
    finally:
        rabbit(True)
    time.sleep(45)
    late = rag_file(f)[0] == 200
    g = upload(ctx["kb"], "apres-panne.txt", text(uniq()))
    t = wait(indexed(f, md5_of(f), [ctx["kb"]]), timeout=180, what="indexed after another change")
    wait(indexed(g, md5_of(g)), what="the other change indexed")
    note(f"indexed 45s after RabbitMQ came back: {late}; with the next change: after {t}s")


@scenario
def F11_unknown_server(ctx, note):
    """Message for an openRAG server missing from RAG_API_KEYS: DLQ at once, the server is never called"""
    f = upload(ctx["kb"], "inconnu.txt", text(uniq()))
    wait(indexed(f, md5_of(f)), what="indexed")
    msg = dict(peek_one_for(f))
    wait(indexed(f, md5_of(f)), what="the real message processed")
    quiet()
    m = mark()
    msg["rag_base_url"] = "http://unknown-openrag.invalid"
    msg["file_id"] = f
    s, body = http("POST", f"{MGMT}/exchanges/%2F/rag.index.topic/publish", {
        "properties": {}, "routing_key": "rag.index.file", "payload": json.dumps(msg), "payload_encoding": "string"},
        MGMT_AUTH)
    assert s == 200 and body.get("routed"), body
    t = wait(lambda: events(m, f, "message_fatal"), timeout=30, what="fatal")
    err = events(m, f, "message_fatal")[0]["error"]
    assert "RAG_API_KEYS" in err and not events(m, f, "rag_api_call") and not events(m, f, "stack_api_call"), err
    note(f"DLQ after {t}s: {err}")
    http("DELETE", f"{MGMT}/queues/%2F/rag.index.dlq/contents", headers=MGMT_AUTH)


if __name__ == "__main__":
    main(setup)
