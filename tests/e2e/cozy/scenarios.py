"""Functional scenarios of the indexing of a cozy-stack instance through rag-indexer.

python3 -I tests/e2e/cozy/scenarios.py [S01 S02 ...]   (all by default, see README.md)
"""
import os
import sys
import time

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


if __name__ == "__main__":
    main(setup)
