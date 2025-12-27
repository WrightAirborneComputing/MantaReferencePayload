# src/portserver/app.py
#!/usr/bin/env python3
"""
PortServer (Android, Chaquopy + Toga)

Bidirectional UDP bridge between two fixed peers (Side A and Side B)
for multiple port pairs.

Port pairs (A_port <-> B_port):
  6000 <-> 6001   (heartbeat/status)
  7000 <-> 7001   (video)
  8000 <-> 8001   (CoT)
  9000 <-> 9001   (extra)

Behavior (NO learning):
- Anything received on local UDP :A_port is forwarded to B_PEER_IP:B_port
- Anything received on local UDP :B_port is forwarded to A_PEER_IP:A_port

Notes:
- Requires AndroidManifest INTERNET permission (and usually ACCESS_NETWORK_STATE).
- Ports 6000-9001 are not privileged; binding is allowed.
"""

import sys
import socket
import threading
import traceback
import queue
import time
from dataclasses import dataclass
from datetime import datetime

import toga
from toga.style import Pack
from toga.style.pack import COLUMN, ROW

# ---- Chaquopy Java bridge (no rubicon, no JavaProxy) ----
from java import dynamic_proxy
from java.lang import Runnable
from android.os import Handler, Looper


# ------------------ Config ------------------
HOST_DEFAULT = "0.0.0.0"   # bind on all interfaces

A_PEER_IP = "192.168.144.50"
B_PEER_IP = "192.168.43.74"

PORT_PAIRS = [
    (6000, 6001),
    (7000, 7001),
    (8000, 8001),
    (9000, 9001),
]

BUF_SIZE = 65535
HEX_PREVIEW_N = 24

def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

@dataclass(frozen=True)
class Peer:
    ip: str
    port: int


# ------------------ UDP Bridge Core ------------------
class UdpFixedBridgePair:
    """
    A single bidirectional UDP bridge for one pair (a_port <-> b_port)
    using fixed forwarding targets (no learning).

    Binds two sockets on `bind_host`:
      - sock_a bound to :a_port
      - sock_b bound to :b_port

    Forwards:
      - recv on sock_a -> send to B_PEER_IP:b_port
      - recv on sock_b -> send to A_PEER_IP:a_port
    """

    def __init__(self, name: str, bind_host: str, a_port: int, b_port: int,
                 a_peer_ip: str, b_peer_ip: str, write=print) -> None:
        self.name = name
        self.bind_host = bind_host
        self.a_port = int(a_port)
        self.b_port = int(b_port)
        self.a_peer = Peer(a_peer_ip, self.a_port)
        self.b_peer = Peer(b_peer_ip, self.b_port)
        self._write = write

        self._sock_a = None
        self._sock_b = None
        self._t_a = None
        self._t_b = None
        self._stop = threading.Event()

    def start(self) -> None:
        if self._t_a and self._t_a.is_alive():
            return

        self._stop.clear()

        self._sock_a = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock_b = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        for s in (self._sock_a, self._sock_b):
            try:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            except Exception:
                pass

        # Bind local listening ports (on Android this binds on device interfaces)
        self._sock_a.bind((self.bind_host, self.a_port))
        self._sock_b.bind((self.bind_host, self.b_port))

        self._t_a = threading.Thread(target=self._run, args=("A",), name=f"{self.name}-A", daemon=True)
        self._t_b = threading.Thread(target=self._run, args=("B",), name=f"{self.name}-B", daemon=True)
        self._t_a.start()
        self._t_b.start()

        self._write(f"[{ts()}] [{self.name}] UDP listening on {self.bind_host}:{self.a_port} and {self.bind_host}:{self.b_port}")
        self._write(f"[{ts()}] [{self.name}] Forward A(:{self.a_port}) -> B_PEER {self.b_peer.ip}:{self.b_peer.port}")
        self._write(f"[{ts()}] [{self.name}] Forward B(:{self.b_port}) -> A_PEER {self.a_peer.ip}:{self.a_peer.port}")

    def stop(self) -> None:
        self._stop.set()
        for s in (self._sock_a, self._sock_b):
            try:
                if s:
                    s.close()
            except Exception:
                pass
        for t in (self._t_a, self._t_b):
            try:
                if t:
                    t.join(timeout=0.5)
            except Exception:
                pass

        self._sock_a = None
        self._sock_b = None
        self._t_a = None
        self._t_b = None

        self._write(f"[{ts()}] [{self.name}] stopped")

    def _run(self, side: str) -> None:
        sock_in = self._sock_a if side == "A" else self._sock_b
        in_port = self.a_port if side == "A" else self.b_port

        dst = self.b_peer if side == "A" else self.a_peer
        tag = f"{self.name}:{side}"

        while not self._stop.is_set():
            try:
                data, src = sock_in.recvfrom(BUF_SIZE)
            except Exception:
                break
            if not data:
                continue

            src_ip, src_port = src[0], int(src[1])
            preview = data[:HEX_PREVIEW_N].hex()

            # Print on receive
            self._write(
                f"[{ts()}] [{tag}] RX {len(data)}B on :{in_port} from {src_ip}:{src_port} "
                f"-> TX to {dst.ip}:{dst.port} preview={preview}"
            )

            try:
                sock_in.sendto(data, (dst.ip, dst.port))
            except Exception as e:
                self._write(f"[{ts()}] [{tag}] TX ERROR to {dst.ip}:{dst.port}: {e}")


# ------------------ Toga App (Android UI + safe logging) ------------------
class PortserverApp(toga.App):
    def startup(self):
        # Keep original stdout for Logcat
        self._orig_stdout = sys.stdout

        # ---- UI ----
        self.host_input = toga.TextInput(value=HOST_DEFAULT, placeholder="Bind host (usually 0.0.0.0)", style=Pack(flex=1))
        self.start_btn = toga.Button("Start", on_press=self.on_start, style=Pack(padding_left=8))
        self.stop_btn = toga.Button("Stop", on_press=self.on_stop, enabled=False, style=Pack(padding_left=8))

        top = toga.Box(
            children=[
                toga.Label("Bind:", style=Pack(padding_right=6)),
                self.host_input,
                self.start_btn,
                self.stop_btn,
            ],
            style=Pack(direction=ROW, padding=8),
        )

        self.console = toga.MultilineTextInput(readonly=True, style=Pack(flex=1, padding=8))
        root = toga.Box(children=[top, self.console], style=Pack(direction=COLUMN, flex=1))

        self.main_window = toga.MainWindow(title=self.formal_name)
        self.main_window.content = root
        self.main_window.show()

        # ---- Android UI posting with Handler(Looper.getMainLooper()) ----
        self._handler = Handler(Looper.getMainLooper())

        class _Poster(dynamic_proxy(Runnable)):
            def __init__(self, fn):
                super().__init__()
                self.fn = fn
            def run(self):
                try:
                    self.fn()
                except Exception:
                    traceback.print_exc()

        self._Poster = _Poster

        # ---- Logging channel ----
        self._console_cache = ""
        self._last_appended_batch = None
        self._log_q = queue.Queue(maxsize=50000)
        self._pump_stop = threading.Event()
        self._pump_thread = threading.Thread(target=self._log_pump, name="UI-LogPump", daemon=True)
        self._pump_thread.start()

        # Runtime state
        self.bridges = []
        self.running = False

        self.log(f"[{ts()}] PortServer ready.")
        self.log(f"[{ts()}] Fixed peers: A={A_PEER_IP}  B={B_PEER_IP}")
        self.log(f"[{ts()}] Port pairs: {PORT_PAIRS}")
        self.log("")

    # ---------- Logging helpers ----------
    def _post_to_ui(self, fn) -> bool:
        try:
            self._handler.post(self._Poster(fn))
            return True
        except Exception as e:
            try:
                self._orig_stdout.write(f"[UI] post failed: {e}\n")
                self._orig_stdout.flush()
            except Exception:
                pass
            return False

    def _log_pump(self):
        keep_last = 200000
        while not self._pump_stop.is_set():
            try:
                first = self._log_q.get(timeout=0.1)
            except queue.Empty:
                continue

            parts = [first]
            while True:
                try:
                    parts.append(self._log_q.get_nowait())
                except queue.Empty:
                    break
            text = "".join(parts)

            def append():
                if text == self._last_appended_batch:
                    return
                self._last_appended_batch = text

                self._console_cache += text
                if len(self._console_cache) > keep_last:
                    self._console_cache = self._console_cache[-keep_last:]

                self.console.value = self._console_cache
                try:
                    native = getattr(getattr(self.console, "_impl", None), "native", None)
                    if native is not None and hasattr(native, "setSelection"):
                        native.setSelection(len(self._console_cache))
                except Exception:
                    pass

            if not self._post_to_ui(append):
                self._pump_stop.wait(0.05)

    def log(self, s: str):
        try:
            if not s.endswith("\n"):
                s += "\n"
            # Logcat
            try:
                self._orig_stdout.write(s)
                self._orig_stdout.flush()
            except Exception:
                pass
            # UI queue
            while True:
                try:
                    self._log_q.put_nowait(s)
                    break
                except queue.Full:
                    try:
                        _ = self._log_q.get_nowait()
                    except queue.Empty:
                        break
        except Exception:
            pass

    def _toggle_buttons_ui(self, start_enabled: bool, stop_enabled: bool):
        def do_toggle():
            self.start_btn.enabled = start_enabled
            self.stop_btn.enabled = stop_enabled
        self._post_to_ui(do_toggle)

    # ---------- Start/stop ----------
    def _start_worker(self, bind_host: str):
        try:
            self.log(f"[{ts()}] Starting UDP bridges on bind_host={bind_host}")
            self.log(f"[{ts()}] Forward targets: A_PEER={A_PEER_IP}  B_PEER={B_PEER_IP}")

            self.bridges = []
            for (a_port, b_port) in PORT_PAIRS:
                name = f"UDP-{a_port}<->{b_port}"
                b = UdpFixedBridgePair(
                    name=name,
                    bind_host=bind_host,
                    a_port=a_port,
                    b_port=b_port,
                    a_peer_ip=A_PEER_IP,
                    b_peer_ip=B_PEER_IP,
                    write=self.log,
                )
                self.bridges.append(b)

            for b in self.bridges:
                b.start()

            self.running = True
            self._toggle_buttons_ui(start_enabled=False, stop_enabled=True)
            self.log(f"[{ts()}] All UDP bridges started.\n")

            # A tiny periodic "still alive" line helps when you think it isn't responding
            def alive():
                while self.running:
                    self.log(f"[{ts()}] [ALIVE] PortServer running.")
                    time.sleep(5.0)
            threading.Thread(target=alive, daemon=True).start()

        except Exception:
            self.log(f"[{ts()}] ERROR starting bridges:\n{traceback.format_exc()}")

    def on_start(self, _):
        if self.running:
            return
        bind_host = (self.host_input.value or "").strip() or HOST_DEFAULT
        self.log(f"[{ts()}] Start pressed.")
        threading.Thread(target=self._start_worker, args=(bind_host,), daemon=True).start()

    def on_stop(self, _):
        if not self.running:
            return
        self.log(f"[{ts()}] Stopping...")

        self.running = False
        for b in self.bridges:
            try:
                b.stop()
            except Exception:
                pass
        self.bridges = []

        self._toggle_buttons_ui(start_enabled=True, stop_enabled=False)
        self.log(f"[{ts()}] Bridges stopped.\n")

    def on_exit(self):
        try:
            self.running = False
            for b in self.bridges:
                try:
                    b.stop()
                except Exception:
                    pass
            self.bridges = []
        finally:
            try:
                self._pump_stop.set()
                if getattr(self, "_pump_thread", None):
                    self._pump_thread.join(timeout=0.5)
            except Exception:
                pass
        return True


def main():
    print("portserver.app: main() called")
    app = PortserverApp("PortServer", "com.aav.portserver")
    print("portserver.app: created App, starting main_loop()")
    app.main_loop()
    return app
