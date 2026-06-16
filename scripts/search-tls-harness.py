#!/usr/bin/env python3
import argparse
import json
import ssl
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse


STORE = {}


class SearchTlsHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path in {"", "/"}:
            self.write_json({"cluster_name": "dbtool-search-tls", "version": {"distribution": "opensearch"}})
            return
        if parsed.path == "/_cluster/health":
            self.write_json({"status": "green"})
            return
        if parsed.path == "/_cat/indices":
            self.write_json([{"index": name} for name in sorted(STORE)])
            return
        self.write_json({"error": "not found"}, status=404)

    def do_POST(self):
        parsed = urlparse(self.path)
        parts = [unquote(part) for part in parsed.path.split("/") if part]
        if len(parts) == 2 and parts[1] == "_doc":
            doc = self.read_json_body()
            docs = STORE.setdefault(parts[0], [])
            docs.append(doc)
            self.write_json({"result": "created", "_index": parts[0], "_id": str(len(docs))})
            return
        if len(parts) == 2 and parts[1] == "_search":
            query = parse_qs(parsed.query)
            body = self.read_json_body()
            size = int(body.get("size") or query.get("size", ["10"])[0])
            offset = int(body.get("from") or query.get("from", ["0"])[0])
            docs = STORE.get(parts[0], [])
            hits = [
                {"_index": parts[0], "_id": str(index + 1), "_source": doc}
                for index, doc in enumerate(docs[offset : offset + size])
            ]
            self.write_json({"hits": {"total": {"value": len(docs)}, "hits": hits}})
            return
        self.write_json({"error": "not found"}, status=404)

    def log_message(self, fmt, *args):
        return

    def read_json_body(self):
        length = int(self.headers.get("content-length", "0"))
        if length == 0:
            return {}
        body = self.rfile.read(length)
        return json.loads(body.decode("utf-8"))

    def write_json(self, payload, status=200):
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.send_header("connection", "close")
        self.end_headers()
        self.wfile.write(body)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9200)
    parser.add_argument("--cert", required=True)
    parser.add_argument("--key", required=True)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), SearchTlsHandler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=args.cert, keyfile=args.key)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
