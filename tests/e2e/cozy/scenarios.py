"""Functional scenarios of the indexing of a cozy-stack instance through rag-indexer.

python3 -I tests/e2e/cozy/scenarios.py [S01 S02 ...]   (all by default, see README.md)
"""
import io
import os
import struct
import sys
import time
import uuid
import zipfile
import zlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lib import *  # noqa: E402,F403

RUN = time.strftime("%H%M%S")
QUIET_DELAY = 20  # trigger debounce (5s) + job + indexer, for the "nothing happens" checks


def setup(ctx):
    ensure_triggers()
    ctx["kb"] = mkdir(ROOT, f"KB-{RUN}")
    ctx["out"] = mkdir(ROOT, f"OUT-{RUN}")
    create_assistant(f"main {RUN}", [ctx["kb"]])
    wait_ws(ctx["kb"])
    print(f"run {RUN}: kb {ctx['kb']}, out {ctx['out']}")


@scenario
def S01_initial(ctx, note):
    """Assistant created on a folder already holding files (subfolders included): all indexed in its workspace"""
    d = mkdir(ROOT, f"KB1-{RUN}")
    sub = mkdir(d, "sous-dossier")
    ids = [upload(d, "a.txt", text(uniq())), upload(d, "b.md", "# Titre\n\n" + text(uniq())),
           upload(sub, "c.txt", text(uniq()))]
    m = mark()
    create_assistant(f"S01 {RUN}", [d])
    for f in ids:
        t = wait(indexed(f, md5_of(f), [d]), what=f"{f} indexed")
    note(f"3 files indexed, last after {t}s; {len(events(m, name='message_received'))} messages received")
    names = {w["workspace_id"]: w["display_name"] for w in rag_workspaces()["workspaces"]}
    assert names.get(d) == f"KB1-{RUN}", f"workspace display_name {names.get(d)!r}"


@scenario
def S02_new_file(ctx, note):
    """New file uploaded into the knowledge base: indexed, searchable"""
    tok = uniq()
    f = upload(ctx["kb"], "nouveau.txt", text(tok))
    t = wait(indexed(f, md5_of(f), [ctx["kb"]]), what="indexed")
    wait(lambda: found(tok, f), timeout=60, what="search hit")
    note(f"indexed after {t}s")
    ctx["s02"] = f


@scenario
def S03_update(ctx, note):
    """Content modified: re-indexed (new md5 on openRAG, new text found, old text gone)"""
    old, new = uniq(), uniq()
    f = upload(ctx["kb"], "modifie.txt", text(old))
    wait(indexed(f, md5_of(f)), what="first version")
    m = mark()
    put_content(f, text(new))
    t = wait(indexed(f, md5_of(f), [ctx["kb"]]), what="new md5")
    wait(lambda: found(new, f), timeout=60, what="new text found")
    assert not found(old, f), "the old text is still found"
    ups = uploads(m, f)
    note(f"re-indexed after {t}s with {[u['method'] for u in ups]}")
    assert any(u["method"] == "PUT" for u in ups), "no PUT"


@scenario
def S04_rename(ctx, note):
    """File renamed, same content: no upload again, still indexed"""
    f = upload(ctx["kb"], "avant.txt", text(uniq()))
    md5 = md5_of(f)
    wait(indexed(f, md5), what="indexed")
    m = mark()
    patch(f, name="apres.txt")
    wait(lambda: events(m, f, "message_processed"), timeout=60, what="sync processed")
    quiet()
    assert not uploads(m, f), "uploaded again"
    assert rag_md5(f) == md5 and rag_ws_of(f) == [ctx["kb"]]
    note(f"openRAG filename after the rename: {rag_file(f)[1]['metadata'].get('filename')!r}")


@scenario
def S05_out_of_scope(ctx, note):
    """File created outside every knowledge base: never published, never indexed"""
    m, ms = mark(), mark(SLOG)
    f = upload(ctx["out"], "dehors.txt", text(uniq()))
    time.sleep(QUIET_DELAY)
    assert rag_file(f)[0] == 404, "indexed"
    assert not events(m, f), "a message was published"
    assert "rag-index" in stack_log(ms), "no rag-index job ran"
    ctx["s05"] = f


@scenario
def S06_move_out(ctx, note):
    """File moved out of the knowledge base: deleted from openRAG"""
    f = upload(ctx["kb"], "sortant.txt", text(uniq()))
    wait(indexed(f, md5_of(f)), what="indexed")
    patch(f, dir_id=ctx["out"])
    note(f"deleted after {wait(gone(f), what='deleted')}s")


@scenario
def S07_move_in(ctx, note):
    """File moved from outside into the knowledge base: indexed"""
    f = ctx.get("s05") or upload(ctx["out"], "entrant.txt", text(uniq()))
    patch(f, dir_id=ctx["kb"])
    note(f"indexed after {wait(indexed(f, md5_of(f), [ctx['kb']]), what='indexed')}s")


@scenario
def S08_trash_restore(ctx, note):
    """File trashed: deleted from openRAG; restored: indexed again"""
    f = upload(ctx["kb"], "corbeille.txt", text(uniq()))
    wait(indexed(f, md5_of(f)), what="indexed")
    trash(f)
    t1 = wait(gone(f), what="deleted")
    restore(f)
    t2 = wait(indexed(f, md5_of(f), [ctx["kb"]]), what="indexed again")
    note(f"deleted after {t1}s, back after {t2}s")


@scenario
def S09_destroy(ctx, note):
    """File trashed then deleted for good: gone from openRAG"""
    f = upload(ctx["kb"], "detruit.txt", text(uniq()))
    wait(indexed(f, md5_of(f)), what="indexed")
    trash(f)
    destroy(f)
    note(f"deleted after {wait(gone(f), what='deleted')}s")


@scenario
def S10_move_dir(ctx, note):
    """Subfolder moved out of the knowledge base then back: its files deleted then indexed again"""
    sub = mkdir(ctx["kb"], f"projet-{RUN}")
    subsub = mkdir(sub, "archives")
    ids = [upload(sub, "p1.txt", text(uniq())), upload(subsub, "p2.txt", text(uniq()))]
    for f in ids:
        wait(indexed(f, md5_of(f)), what="indexed")
    patch(sub, dir_id=ctx["out"])
    for f in ids:
        t1 = wait(gone(f), what=f"{f} deleted")
    patch(sub, dir_id=ctx["kb"])
    for f in ids:
        t2 = wait(indexed(f, md5_of(f), [ctx["kb"]]), what=f"{f} indexed again")
    note(f"deleted after {t1}s, back after {t2}s (2 levels)")


@scenario
def S11_rename_kb(ctx, note):
    """Knowledge base folder renamed: its files stay indexed, nothing uploaded again"""
    d = mkdir(ROOT, f"KB11-{RUN}")
    f = upload(d, "x.txt", text(uniq()))
    create_assistant(f"S11 {RUN}", [d])
    wait(indexed(f, md5_of(f), [d]), what="indexed")
    m = mark()
    patch(d, name=f"KB11-renomme-{RUN}")
    time.sleep(QUIET_DELAY)
    quiet()
    assert rag_ws_of(f) == [d], f"workspaces {rag_ws_of(f)}"
    assert not uploads(m), "uploaded again"
    names = {w["workspace_id"]: w["display_name"] for w in rag_workspaces()["workspaces"]}
    note(f"{len(events(m, name='message_received'))} sync message(s); workspace display_name now {names.get(d)!r}")


@scenario
def S12_nested(ctx, note):
    """Assistant on a subfolder of the knowledge base: its files in both workspaces; assistant deleted: back to one"""
    sub = mkdir(ctx["kb"], f"imbrique-{RUN}")
    f_in = upload(sub, "dedans.txt", text(uniq()))
    f_top = upload(ctx["kb"], "dessus.txt", text(uniq()))
    wait(indexed(f_in, md5_of(f_in), [ctx["kb"]]), what="indexed in kb")
    a = create_assistant(f"S12 {RUN}", [sub])
    t = wait(indexed(f_in, None, [ctx["kb"], sub]), what="in both workspaces")
    wait(indexed(f_top, None, [ctx["kb"]]), what="the file above stays in kb only")
    delete_assistant(a)
    wait_ws(sub, present=False)
    t2 = wait(indexed(f_in, None, [ctx["kb"]]), what="back to the kb workspace only")
    note(f"both workspaces after {t}s, back to one after {t2}s")


@scenario
def S13_shared_folder(ctx, note):
    """Two assistants on the same folder, one deleted: workspace and files kept"""
    a = create_assistant(f"S13 {RUN}", [ctx["kb"]])
    f = ctx.get("s02") or upload(ctx["kb"], "partage.txt", text(uniq()))
    wait(indexed(f, None, [ctx["kb"]]), what="indexed")
    time.sleep(QUIET_DELAY)
    delete_assistant(a)
    time.sleep(QUIET_DELAY)
    quiet()
    assert rag_ws_of(f) == [ctx["kb"]], f"workspaces {rag_ws_of(f)}"
    wait_ws(ctx["kb"], present=True)


@scenario
def S14_delete_assistant(ctx, note):
    """Last assistant of a folder deleted: workspace removed, its files deleted from openRAG"""
    d = mkdir(ROOT, f"KB14-{RUN}")
    ids = [upload(d, f"{i}.txt", text(uniq())) for i in range(2)]
    a = create_assistant(f"S14 {RUN}", [d])
    for f in ids:
        wait(indexed(f, md5_of(f), [d]), what="indexed")
    delete_assistant(a)
    t = wait_ws(d, present=False)
    for f in ids:
        wait(gone(f), what=f"{f} deleted")
    note(f"workspace removed after {t}s, files deleted (by the stack directly, not through the indexer)")


@scenario
def S15_unsupported(ctx, note):
    """Formats openRAG refuses (.bin, .xyz): never downloaded, acked, status notsupported posted back to the stack"""
    m = mark()
    ids = {"donnees.bin": upload(ctx["kb"], "donnees.bin", os.urandom(2048), "application/octet-stream"),
           "fichier.xyz": upload(ctx["kb"], "fichier.xyz", text(uniq()), "application/octet-stream")}
    for name, f in ids.items():
        wait(lambda: rag_status(f), timeout=120, what=f"status of {name}")
        st = rag_status(f)
        note(f"{name}: status {st.get('status')!r}, docRev {st.get('docRev')} (file rev {file_doc(f)['meta']['rev']}), "
             f"openRAG {rag_file(f)[0]}")
        assert st.get("status") == "notsupported", st
        assert not events(m, f, "file_download"), f"{name} downloaded"


@scenario
def S16_image(ctx, note):
    """Image without the rag.index.image.enabled flag: notsupported set by the stack, nothing published"""
    m = mark()
    f = upload(ctx["kb"], "image.png", png(), "image/png")
    wait(lambda: rag_status(f), timeout=60, what="status")
    assert rag_status(f).get("status") == "notsupported"
    time.sleep(5)
    assert not events(m, f), "published"


@scenario
def S17_duplicate(ctx, note):
    """Second file with the content of an indexed one: acked, never in the DLQ (openRAG dedup or not)"""
    content = text(uniq())
    f1 = upload(ctx["kb"], "original.txt", content)
    wait(indexed(f1, md5_of(f1)), what="original indexed")
    m = mark()
    f2 = upload(ctx["kb"], "copie.txt", content)
    wait(lambda: events(m, f2, "message_processed") or events(m, f2, "message_fatal"), what="copy processed")
    quiet()
    assert not events(m, f2, "message_fatal") and not [x for x in peek("rag.index.dlq") if x.get("file_id") == f2]
    if events(m, f2, "rag_upsert_duplicate_content"):
        assert rag_file(f2)[0] == 404, "copy indexed"
        note("openRAG refused the copy (DOCUMENT_CONTENT_EXISTS): not indexed")
    else:
        note(f"openRAG has no content dedup here: copy indexed too ({rag_file(f2)[0]}, ws {rag_ws_of(f2)})")


NOTE_SCHEMA = {"nodes": [["doc", {"content": "block+"}], ["paragraph", {"content": "inline*", "group": "block"}],
                         ["text", {"group": "inline"}]], "marks": [], "topNode": "doc"}


@scenario
def S18_note(ctx, note):
    """Cozy note in the knowledge base: indexed as markdown, its text searchable"""
    tok = uniq()
    s, body = stack("POST", "/notes", {"data": {"type": "io.cozy.notes.documents", "attributes": {
        "title": f"Compte rendu {RUN}", "dir_id": ctx["kb"], "schema": NOTE_SCHEMA}}}, ctype="application/vnd.api+json")
    f = ok(s, body)["data"]["id"]
    steps = {"data": [{"type": "io.cozy.notes.steps", "attributes": {
        "sessionID": "1", "stepType": "replace", "from": 1, "to": 1,
        "slice": {"content": [{"type": "text", "text": f"Decision du comite : le code est {tok}."}]}}}]}
    ok(*http("PATCH", f"{STACK}/notes/{f}", steps, {"Authorization": f"Bearer {TOKEN}", "If-Match": "0",
                                                   "Content-Type": "application/vnd.api+json"}))
    ok(*stack("POST", f"/notes/{f}/sync"), 200, 204)
    t = wait(lambda: found(tok, f), what="note text found")
    meta = rag_file(f)[1]["metadata"]
    note(f"found after {t}s; openRAG filename {meta.get('filename')!r}")
    assert meta.get("filename", "").endswith(".md")


@scenario
def S19_burst(ctx, note):
    """Three updates in a row: the last content is the one indexed"""
    f = upload(ctx["kb"], "rafale.txt", text(uniq()))
    for _ in range(3):
        last = uniq()
        put_content(f, text(last))
        time.sleep(0.5)
    t = wait(indexed(f, md5_of(f)), what="last md5")
    wait(lambda: found(last, f), timeout=60, what="last text found")
    note(f"last version indexed after {t}s")


@scenario
def S20_bulk(ctx, note):
    """40 files uploaded at once: all indexed"""
    d = mkdir(ctx["kb"], f"lot-{RUN}")
    m = mark()
    start = time.time()
    ids = [upload(d, f"lot-{i:02}.txt", text(uniq())) for i in range(40)]
    up = round(time.time() - start, 1)
    for f in ids:
        wait(indexed(f, md5_of(f), [ctx["kb"]]), what=f"{f} indexed")
    retries = len(events(m, name="message_retry"))
    note(f"uploaded in {up}s, all indexed {round(time.time() - start, 1)}s after the first upload; "
         f"{len(events(m, name='message_received'))} deliveries, {retries} retries")


@scenario
def S21_big(ctx, note):
    """3 MB text file: streamed and indexed"""
    tok = uniq()
    body = (text(tok) + "".join(f"Ligne {i} de remplissage {uuid.uuid4()} pour grossir le fichier.\n"
                                for i in range(45000)))
    f = upload(ctx["kb"], "gros.txt", body)
    note(f"{len(body) // 1024} KB, indexed after {wait(indexed(f, md5_of(f)), timeout=600, what='indexed')}s")


@scenario
def S22_office(ctx, note):
    """PDF and DOCX: indexed, their text searchable"""
    for name, mime, make in (("rapport.pdf", "application/pdf", pdf),
                             ("note.docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document", docx)):
        tok = uniq()
        f = upload(ctx["kb"], name, make(f"Le code secret du rapport est {tok}."), mime)
        t = wait(indexed(f, md5_of(f)), what=f"{name} indexed")
        t2 = wait(lambda: found(tok, f), timeout=120, what=f"{name} text found")
        note(f"{name}: indexed after {t}s, found after {t2}s more")


def duplicates(since):
    """Files openRAG refused as a copy of another (S17): never stored, so sent again."""
    return {e["file_id"] for e in events(since, name="rag_upsert_duplicate_content")}


@scenario
def S23_reset(ctx, note):
    """Admin reset: the whole feed published again, nothing uploaded again (md5 unchanged)"""
    quiet()
    m = mark()
    s, body = admin("POST", "/rag/reset")
    assert s in (200, 202, 204), (s, body)
    wait(lambda: events(m, name="message_received"), what="messages")
    quiet(seconds=10)
    recv = events(m, name="message_received")
    ups = uploads(m)
    dup = duplicates(m)
    note(f"{len(recv)} messages, {len({e['file_id'] for e in recv})} files, {len(ups)} uploads "
         f"({len(dup)} of copies openRAG refuses as duplicates, sent again)")
    other = [(u["file_id"], u["method"]) for u in ups if u["file_id"] not in dup]
    assert not other, f"uploads: {other[:5]}"


@scenario
def S24_reconcile(ctx, note):
    """Admin reconcile of one folder: its files published again, nothing uploaded again"""
    quiet()
    m = mark()
    s, body = admin("POST", f"/rag/reconcile?dir_id={ctx['kb']}")
    assert s in (200, 202, 204), (s, body)
    wait(lambda: events(m, name="message_received"), what="messages")
    quiet(seconds=10)
    dup = duplicates(m)
    note(f"answer {body}; {len(events(m, name='message_received'))} messages, {len(uploads(m))} uploads "
         f"({len(dup)} of copies openRAG refuses as duplicates)")
    assert not [u for u in uploads(m) if u["file_id"] not in dup]


@scenario
def S25_prune(ctx, note):
    """Admin prune: the files still claimed by a knowledge base stay on openRAG"""
    quiet()
    f = ctx.get("s02")
    s, body = admin("POST", "/rag/prune")
    assert s in (200, 202, 204), (s, body)
    note(f"answer {body}")
    quiet()
    if f:
        assert rag_ws_of(f) == [ctx["kb"]]


@scenario
def S26_status_route(ctx, note):
    """POST /ai/index/status (what openRAG would post): saved, older revision ignored, bad payloads 400"""
    f = upload(ctx["kb"], "statut.txt", text(uniq()))
    put_content(f, text(uniq()))
    rev = file_doc(f)["meta"]["rev"]
    wait(indexed(f, md5_of(f)), what="indexed")

    def post(**kw):
        body = {"partition": DOMAIN, "file_id": f, "status": "success", "metadata": {"doc_rev": rev}}
        body.update(kw)
        return http("POST", f"{STACK}/ai/index/status", body)[0]
    assert post() == 204 and rag_status(f)["status"] == "success" and rag_status(f)["docRev"] == rev
    assert post(status="error", metadata={"doc_rev": "1-aaa"}) == 204 and rag_status(f)["status"] == "success"
    assert post(partition="other.example") == 400
    assert post(metadata={}) == 400
    assert post(status="pasunstatut") == 400


@scenario
def S27_names(ctx, note):
    """Names with spaces, accents and quotes: indexed (the filename openRAG keeps is reported)"""
    for name in ("Réunion équipe été.txt", "rapport final (v2).md", "l'offre d'emploi.txt"):
        f = upload(ctx["kb"], name, text(uniq()))
        wait(indexed(f, md5_of(f)), what=f"{name} indexed")
        got = rag_file(f)[1]["metadata"].get("filename")
        note(f"{name!r} -> openRAG filename {got!r}")


# --- fixtures ---------------------------------------------------------------------

def png():
    def chunk(t, d):
        return struct.pack(">I", len(d)) + t + d + struct.pack(">I", zlib.crc32(t + d))
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(b"\x00" + os.urandom(3))) + chunk(b"IEND", b""))


def pdf(line):
    stream = f"BT /F1 14 Tf 72 720 Td ({line}) Tj ET".encode()
    objs = [b"<< /Type /Catalog /Pages 2 0 R >>", b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R "
            b"/Resources << /Font << /F1 5 0 R >> >> >>",
            b"<< /Length %d >>\nstream\n" % len(stream) + stream + b"\nendstream",
            b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"]
    out, offsets = b"%PDF-1.4\n", []
    for i, o in enumerate(objs, 1):
        offsets.append(len(out))
        out += b"%d 0 obj\n" % i + o + b"\nendobj\n"
    xref = len(out)
    out += b"xref\n0 %d\n0000000000 65535 f \n" % (len(objs) + 1)
    out += b"".join(b"%010d 00000 n \n" % o for o in offsets)
    return out + b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n" % (len(objs) + 1, xref)


def docx(line):
    """The smallest .docx Word and openRAG open: one paragraph."""
    ns = 'xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"'
    parts = {
        "[Content_Types].xml": '<?xml version="1.0"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/'
        'content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships'
        '+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/word/document.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/></Types>',
        "_rels/.rels": '<?xml version="1.0"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/'
        'relationships"><Relationship Id="r1" Type="http://schemas.openxmlformats.org/officeDocument/2006/'
        'relationships/officeDocument" Target="word/document.xml"/></Relationships>',
        "word/document.xml": f'<?xml version="1.0"?><w:document {ns}><w:body><w:p><w:r><w:t>{line}</w:t></w:r>'
        '</w:p></w:body></w:document>',
    }
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as z:
        for name, xml in parts.items():
            z.writestr(name, xml)
    return out.getvalue()


if __name__ == "__main__":
    main(setup)
