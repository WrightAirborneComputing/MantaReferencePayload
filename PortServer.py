#!/usr/bin/env python3
import socket
import socketserver
import threading
import sys
from datetime import datetime

HOST_DEFAULT = "0.0.0.0"

# name, tcp_a, tcp_b
BRIDGE_CONFIGS = [
    ("BRIDGE-0", 6000, 7000),
    ("BRIDGE-1", 6001, 7001),
    ("BRIDGE-2", 6002, 7002),
]

def ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def safe_preview(b: bytes, max_len: int = 200) -> str:
    try:
        s = b.decode("utf-8", errors="replace")
    except Exception:
        s = "<decode error>"
    if len(s) > max_len:
        s = s[:max_len] + f"...(+{len(s)-max_len} more)"
    return s.replace("\r", "\\r").replace("\n", "\\n").replace("\t", "\\t")

class ThreadingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True

class Bridge:
    """
    A pure TCP bridge:
      - Clients connect to TCP_A or TCP_B.
      - Any data arriving on one side is broadcast to all clients on the opposite side.
    """
    def __init__(self, host: str, name: str, tcp_a: int, tcp_b: int):
        self.host = host
        self.name = name
        self.tcp_a = tcp_a
        self.tcp_b = tcp_b

        # TCP client sets
        self._clients_a = set()
        self._clients_b = set()
        self._clients_lock = threading.RLock()

        # Servers & threads
        self._tcp_srv_a = None
        self._tcp_srv_b = None
        self._tcp_threads = []
        self._stop_event = threading.Event()

    # ---- TCP internals ----
    def _add_client(self, sock: socket.socket, which: str):
        with self._clients_lock:
            (self._clients_a if which == 'A' else self._clients_b).add(sock)

    def _remove_client(self, sock: socket.socket, which: str):
        with self._clients_lock:
            try:
                (self._clients_a if which == 'A' else self._clients_b).discard(sock)
            finally:
                try: sock.close()
                except Exception: pass

    def _forward_tcp(self, to_set: set, data: bytes) -> int:
        ok, dead = 0, []
        with self._clients_lock:
            for s in list(to_set):
                try:
                    s.sendall(data)
                    ok += 1
                except Exception:
                    dead.append(s)
            for s in dead:
                try: to_set.discard(s)
                except Exception: pass
                try: s.close()
                except Exception: pass
        return ok

    def _make_tcp_handler(self, side: str):
        bridge = self

        class _Handler(socketserver.BaseRequestHandler):
            def setup(self):
                peer = f"{self.client_address[0]}:{self.client_address[1]}"
                port = bridge.tcp_a if side == 'A' else bridge.tcp_b
                print(f"[{ts()}] [{bridge.name}] TCP connect on {bridge.host}:{port} from {peer}", flush=True)
                bridge._add_client(self.request, side)
                try:
                    self.request.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                except Exception:
                    pass

            def handle(self):
                peer = f"{self.client_address[0]}:{self.client_address[1]}"
                in_port = bridge.tcp_a if side == 'A' else bridge.tcp_b
                out_port = bridge.tcp_b if side == 'A' else bridge.tcp_a
                while True:
                    try:
                        data = self.request.recv(65536)
                    except Exception as e:
                        print(f"[{ts()}] [{bridge.name}] TCP recv error from {peer} on {bridge.host}:{in_port}: {e}", flush=True)
                        break
                    if not data:
                        print(f"[{ts()}] [{bridge.name}] TCP disconnect {peer} on {bridge.host}:{in_port}", flush=True)
                        break

                    print(f"[{ts()}] [{bridge.name}] TCP {len(data)}B from {peer} on {bridge.host}:{in_port}")
                    # print(f"           UTF-8 preview: {safe_preview(data)}")

                    if side == 'A':
                        sent = bridge._forward_tcp(bridge._clients_b, data)
                    else:
                        sent = bridge._forward_tcp(bridge._clients_a, data)

                    if sent == 0:
                        print(f"[{ts()}] [{bridge.name}] TCP: no clients on {bridge.host}:{out_port}; dropped.", flush=True)
                    else:
                        print(f"[{ts()}] [{bridge.name}] TCP: forwarded to {sent} client(s) on {bridge.host}:{out_port}", flush=True)

            def finish(self):
                bridge._remove_client(self.request, side)

        return _Handler

    def _start_tcp(self):
        handler_a = self._make_tcp_handler('A')
        handler_b = self._make_tcp_handler('B')
        self._tcp_srv_a = ThreadingTCPServer((self.host, self.tcp_a), handler_a)
        self._tcp_srv_b = ThreadingTCPServer((self.host, self.tcp_b), handler_b)
        tA = threading.Thread(target=self._tcp_srv_a.serve_forever, daemon=True)
        tB = threading.Thread(target=self._tcp_srv_b.serve_forever, daemon=True)
        tA.start(); tB.start()
        self._tcp_threads.extend([tA, tB])
        print(f"[{ts()}] [{self.name}] TCP listening on {self.host}:{self.tcp_a} and {self.host}:{self.tcp_b}")

    # ---- Public API ----
    def start(self):
        self._stop_event.clear()
        self._start_tcp()

    def stop(self):
        self._stop_event.set()

        # stop TCP servers
        for srv in (self._tcp_srv_a, self._tcp_srv_b):
            try: srv.shutdown(); srv.server_close()
            except Exception: pass

        # close clients
        with self._clients_lock:
            for s in list(self._clients_a):
                try: s.close()
                except Exception: pass
            for s in list(self._clients_b):
                try: s.close()
                except Exception: pass

        # wait briefly for threads to unwind
        for t in self._tcp_threads:
            try: t.join(timeout=0.5)
            except Exception: pass

def usage():
    print("Usage: python bridge_tcp_only.py [host]\n"
          f"Default host: {HOST_DEFAULT}\n" +
          "\n".join([f"  {name}: TCP {ta} ⇄ {tb}" for name, ta, tb in BRIDGE_CONFIGS]))

def main():
    host = HOST_DEFAULT
    if len(sys.argv) >= 2 and sys.argv[1] in ("-h", "--help"):
        usage(); return
    if len(sys.argv) >= 2:
        host = sys.argv[1]

    bridges = [Bridge(host, name, ta, tb) for (name, ta, tb) in BRIDGE_CONFIGS]

    print(f"[{ts()}] Starting {len(bridges)} TCP bridges on host {host}")
    for b in bridges:
        print(f"  [{b.name}] TCP {host}:{b.tcp_a} ⇄ {host}:{b.tcp_b}")
    print(f"[{ts()}] TCP: data from one side is broadcast to all clients on the opposite side.\n"
          f"Press Ctrl+C to stop.\n", flush=True)

    for b in bridges:
        b.start()

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print(f"\n[{ts()}] Shutting down...")
    finally:
        for b in bridges:
            b.stop()
        print(f"[{ts()}] Bridges stopped.")

if __name__ == "__main__":
    main()
