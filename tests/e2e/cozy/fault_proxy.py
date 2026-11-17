"""Reverse proxy to an openRAG server that fails on demand, on 127.0.0.1:<port>.

The mode is read from the mode file on each request:
  ok          forward everything
  indexer:503 answer 503 to the calls only the indexer makes (/indexer/..., GET .../file/<id>)
  indexer:403 same with 403
  all:503     answer 503 to everything (the stack's workspace calls too)
Usage: python3 -I fault_proxy.py <upstream openRAG URL> <port> <mode file>
"""
import re
import sys
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UPSTREAM = sys.argv[1].rstrip("/")
PORT = int(sys.argv[2])
MODE_FILE = sys.argv[3]
INDEXER_ONLY = re.compile(r"^/indexer/|^/partition/[^/]+/file/")


def mode():
    try:
        return open(MODE_FILE).read().strip() or "ok"
    except OSError:
        return "ok"


class Proxy(BaseHTTPRequestHandler):
    def _handle(self):
        m = mode()
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if m != "ok":
            scope, code = m.split(":")
            if scope == "all" or INDEXER_ONLY.search(self.path):
                self.log_message("FAULT %s %s -> %s", self.command, self.path, code)
                self._reply(int(code), b'{"detail":"injected fault"}', {"Content-Type": "application/json"})
                return
        headers = {k: v for k, v in self.headers.items() if k.lower() in ("authorization", "content-type", "accept")}
        req = urllib.request.Request(UPSTREAM + self.path, data=body or None, method=self.command, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                self._reply(r.status, r.read(), r.headers)
        except urllib.error.HTTPError as e:
            self._reply(e.code, e.read(), e.headers)
        except OSError as e:
            self._reply(502, str(e).encode(), {})

    def _reply(self, code, content, headers):
        self.send_response(code)
        self.send_header("Content-Type", headers.get("Content-Type", "application/json"))
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    do_GET = do_POST = do_PUT = do_DELETE = do_PATCH = _handle


ThreadingHTTPServer(("127.0.0.1", PORT), Proxy).serve_forever()
