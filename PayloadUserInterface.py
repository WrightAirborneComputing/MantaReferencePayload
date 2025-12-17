#!/usr/bin/env python3
"""
GUI H.264/MPEG-TS viewer with side-by-side console.

- Connects to multiple TCP ports (monitor-only for non-display ports).
- Decodes/display stream from DISPLAY_PORT inside the GUI (no external window).
- Overlays resolution, FPS (smoothed), codec, and transport info on the video.
- Redirects stdout/stderr to an on-screen console pane (right side).
- NEW: Forwards all data from SERVER_IP:7002 to 127.0.0.1:18087.
"""

import sys
import os
import socket
import threading
import time
from collections import deque
from datetime import datetime
from typing import Optional

# --- Third-party ---
# pip install pyqt5 av opencv-python numpy
import av
import numpy as np
import cv2
from PyQt5 import QtCore, QtGui, QtWidgets

# ------------------ Config ------------------
SERVER_IP = "192.168.144.11"

# Ports to monitor (stdout preview only). We EXCLUDE 7002 so the forwarder can own it.
TCP_PORTS = [7000, 7001]

# Port that carries H.264-in-MPEGTS we want to view in the GUI.
DISPLAY_PORT = 7001

# Forward 7002 -> localhost:18087
FORWARD_SRC_PORT = 7002
FORWARD_DST_HOST = "127.0.0.1"
FORWARD_DST_PORT = 18087

WINDOW_TITLE = "MPEG-TS Viewer + Console"
INITIAL_WIN_W, INITIAL_WIN_H = 1200, 700

# --------------- Utilities ------------------
def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def safe_preview(b: bytes, max_len: int = 200) -> str:
    try:
        s = b.decode("utf-8", errors="replace")
    except Exception:
        s = "<decode error>"
    if len(s) > max_len:
        s = s[:max_len] + f"...(+{len(s)-max_len} more)"
    return s.replace("\r", "\\r").replace("\n", "\\n").replace("\t", "\\t")

# ---------- Qt console redirection ----------
class EmittingStream(QtCore.QObject):
    text_ready = QtCore.pyqtSignal(str)

    def write(self, text):
        if text:
            self.text_ready.emit(str(text))

    def flush(self):
        pass

class ConsoleWidget(QtWidgets.QTextEdit):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setReadOnly(True)
        self.setLineWrapMode(QtWidgets.QTextEdit.NoWrap)
        self.setFont(QtGui.QFont("Consolas" if sys.platform.startswith("win") else "Monospace", 10))
        pal = self.palette()
        pal.setColor(QtGui.QPalette.Base, QtGui.QColor(15, 15, 15))
        pal.setColor(QtGui.QPalette.Text, QtGui.QColor(220, 220, 220))
        self.setPalette(pal)

    @QtCore.pyqtSlot(str)
    def append_text(self, text: str):
        self.moveCursor(QtGui.QTextCursor.End)
        self.insertPlainText(text)
        self.moveCursor(QtGui.QTextCursor.End)

# -------------- Video display ---------------
class VideoWidget(QtWidgets.QLabel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAlignment(QtCore.Qt.AlignCenter)
        self.setMinimumSize(320, 240)
        self.setStyleSheet("QLabel { background-color: #000; }")

    def set_frame(self, frame_bgr: np.ndarray):
        if frame_bgr is None:
            return
        h, w = frame_bgr.shape[:2]
        rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
        qimg = QtGui.QImage(rgb.data, w, h, rgb.strides[0], QtGui.QImage.Format_RGB888)
        pix = QtGui.QPixmap.fromImage(qimg)
        self.setPixmap(pix.scaled(self.size(), QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation))

    def resizeEvent(self, event):
        pm = self.pixmap()
        if pm:
            self.setPixmap(pm.scaled(self.size(), QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation))
        super().resizeEvent(event)

# --------- Decoder thread (PyAV) ------------
class DecoderThread(QtCore.QThread):
    frame_ready = QtCore.pyqtSignal(np.ndarray)   # emits BGR image

    def __init__(self, host: str, port: int, parent=None):
        super().__init__(parent)
        self.host = host
        self.port = port
        self._stop = threading.Event()

        self._ts_window = deque(maxlen=90)
        self._nominal_fps: Optional[float] = None
        self._width: Optional[int] = None
        self._height: Optional[int] = None
        self._codec: str = "unknown"

    def stop(self):
        self._stop.set()

    def _draw_overlay(self, img: np.ndarray) -> np.ndarray:
        h, w = img.shape[:2]

        now = time.time()
        self._ts_window.append(now)
        rt_fps = None
        if len(self._ts_window) >= 2:
            elapsed = self._ts_window[-1] - self._ts_window[0]
            if elapsed > 0:
                rt_fps = (len(self._ts_window) - 1) / elapsed

        res_text = f"Resolution: {self._width}x{self._height}" if (self._width and self._height) else "Resolution: (detecting...)"
        fps_nom_text = f"{self._nominal_fps:.2f} fps" if self._nominal_fps else "unknown"
        fps_rt_text = f"{rt_fps:.2f} fps" if rt_fps else "estimating..."
        info_line = f"Frame rate: {fps_rt_text} (nominal: {fps_nom_text})"
        codec_text = f"Codec: {self._codec}"
        label_text = f"H.264 / MPEG-TS / TCP  @ {self.host}:{self.port}"

        lines = [res_text, info_line, codec_text, label_text]

        margin = 8
        line_h = 24
        band_h = margin * 2 + line_h * len(lines)
        band_w = max(cv2.getTextSize(t, cv2.FONT_HERSHEY_DUPLEX, 0.6, 1)[0][0] for t in lines) + margin * 2
        band_w = min(band_w, w)

        overlay = img.copy()
        cv2.rectangle(overlay, (0, 0), (band_w, band_h), (0, 0, 0), thickness=-1)
        cv2.addWeighted(overlay, 0.45, img, 0.55, 0.0, dst=img)

        y = margin + line_h - 6
        for text in lines:
            cv2.putText(img, text, (margin, y), cv2.FONT_HERSHEY_DUPLEX, 0.6, (255, 255, 255), 1, cv2.LINE_AA)
            y += line_h

        return img

    def run(self):
        url = f"tcp://{self.host}:{self.port}"
        print(f"[{ts()}] [DECODE] Opening {url} (mpegts, h264)...")
        try:
            container = av.open(url, format="mpegts", timeout=None)
        except Exception as e:
            print(f"[{ts()}] [DECODE] Failed to open: {e}\n")
            return

        video_stream = next((s for s in container.streams if s.type == "video"), None)
        if not video_stream:
            print(f"[{ts()}] [DECODE] No video stream found.")
            try: container.close()
            except Exception: pass
            return

        try:
            self._codec = getattr(video_stream.codec_context, "name", "unknown") or "unknown"
        except Exception:
            self._codec = "unknown"

        try:
            if video_stream.average_rate is not None:
                self._nominal_fps = float(video_stream.average_rate)
        except Exception:
            self._nominal_fps = None

        try:
            video_stream.thread_type = "AUTO"
        except Exception:
            pass

        print(f"[{ts()}] [DECODE] Stream ready. Codec={self._codec}, nominal_fps={self._nominal_fps}")

        try:
            for packet in container.demux(video_stream):
                if self._stop.is_set():
                    break
                if packet.stream.type != "video":
                    continue

                for frame in packet.decode():
                    if self._stop.is_set():
                        break

                    if not self._width or not self._height:
                        self._width, self._height = frame.width, frame.height

                    img = frame.to_ndarray(format="bgr24")
                    img = self._draw_overlay(img)

                    self.frame_ready.emit(img)
        except av.error.ExitError as e:
            print(f"[{ts()}] [DECODE] FFmpeg/AV exit: {e}")
        except Exception as e:
            print(f"[{ts()}] [DECODE] Error: {e}")
        finally:
            try:
                container.close()
            except Exception:
                pass
            print(f"[{ts()}] [DECODE] Stopped.")

# ---------- TCP monitor threads -------------
def tcp_listener(stop_event: threading.Event, host: str, port: int, display: bool = False):
    if display:
        return

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print(f"[{ts()}] [TCP:{port}] Connecting to {host}:{port}...")
    try:
        sock.connect((host, port))
    except Exception as e:
        print(f"[{ts()}] [TCP:{port}] Connection failed: {e}")
        return
    print(f"[{ts()}] [TCP:{port}] Connected. Listening for data...")

    try:
        while not stop_event.is_set():
            data = sock.recv(65536)
            if not data:
                print(f"[{ts()}] [TCP:{port}] Server closed connection.")
                break
            print(f"[{ts()}] [TCP:{port}] >>> {len(data)} bytes [{safe_preview(data)}]")
    except Exception as e:
        print(f"[{ts()}] [TCP:{port}] Error: {e}")
    finally:
        try: sock.close()
        except Exception: pass
        print(f"[{ts()}] [TCP:{port}] Disconnected.")

# ---------- TCP forwarder (7002 -> 127.0.0.1:18087) ----------
def tcp_forwarder(stop_event: threading.Event, src_host: str, src_port: int,
                  dst_host: str, dst_port: int, reconnect_delay_s: float = 2.0):
    """
    One-way forwarder: read from (src_host, src_port) and send to (dst_host, dst_port).
    Reconnects on errors until stop_event is set.
    """
    print(f"[{ts()}] [FWD] Starting forwarder {src_host}:{src_port}  -->  {dst_host}:{dst_port}")

    while not stop_event.is_set():
        src = None
        dst = None
        try:
            # Connect source
            src = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            src.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            print(f"[{ts()}] [FWD] Connecting to source {src_host}:{src_port} ...")
            src.connect((src_host, src_port))
            print(f"[{ts()}] [FWD] Source connected.")

            # Connect destination
            dst = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            dst.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            print(f"[{ts()}] [FWD] Connecting to destination {dst_host}:{dst_port} ...")
            dst.connect((dst_host, dst_port))
            print(f"[{ts()}] [FWD] Destination connected. Forwarding data...")

            total = 0
            while not stop_event.is_set():
                chunk = src.recv(65536)
                if not chunk:
                    print(f"[{ts()}] [FWD] Source closed connection.")
                    break
                print(f"[{ts()}] [Fwd: {len(chunk)} bytes [{safe_preview(chunk)}]")
                dst.sendall(chunk)
                total += len(chunk)
        except Exception as e:
            print(f"[{ts()}] [FWD] Error: {e}")
        finally:
            if dst:
                try: dst.close()
                except Exception: pass
            if src:
                try: src.close()
                except Exception: pass
            if not stop_event.is_set():
                print(f"[{ts()}] [FWD] Reconnecting in {reconnect_delay_s:.1f}s...")
                time.sleep(reconnect_delay_s)

    print(f"[{ts()}] [FWD] Forwarder stopped.")

# -------------- Main window ----------------
class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, host: str, ports: list[int], display_port: int):
        super().__init__()
        self.setWindowTitle(WINDOW_TITLE)
        self.resize(INITIAL_WIN_W, INITIAL_WIN_H)

        central = QtWidgets.QWidget(self)
        outer_vbox = QtWidgets.QVBoxLayout(central)
        outer_vbox.setContentsMargins(8, 8, 8, 8)
        outer_vbox.setSpacing(8)

        # Header
        header = QtWidgets.QLabel(
            f"<b>Server:</b> {host} | <b>Display Port:</b> {display_port} | "
            f"<b>Monitors:</b> {', '.join(map(str, ports))} | "
            f"<b>Forward:</b> {host}:{FORWARD_SRC_PORT} → {FORWARD_DST_HOST}:{FORWARD_DST_PORT}"
        )
        outer_vbox.addWidget(header)

        # Splitter (horizontal): left = video, right = console
        splitter = QtWidgets.QSplitter(QtCore.Qt.Horizontal)

        # Left: Video group
        video_group = QtWidgets.QGroupBox("Video")
        video_layout = QtWidgets.QVBoxLayout(video_group)
        self.video_widget = VideoWidget()
        video_layout.addWidget(self.video_widget)
        splitter.addWidget(video_group)

        # Right: Console group
        console_group = QtWidgets.QGroupBox("Console (stdout / stderr)")
        console_layout = QtWidgets.QVBoxLayout(console_group)
        self.console = ConsoleWidget()
        console_layout.addWidget(self.console)
        splitter.addWidget(console_group)

        # Initial sizes and stretch
        splitter.setStretchFactor(0, 3)  # video gets more space
        splitter.setStretchFactor(1, 2)
        splitter.setSizes([int(INITIAL_WIN_W * 0.65), int(INITIAL_WIN_W * 0.35)])

        outer_vbox.addWidget(splitter, stretch=1)
        self.setCentralWidget(central)

        # Redirect stdout/stderr
        self.stdout_stream = EmittingStream()
        self.stderr_stream = EmittingStream()
        self.stdout_stream.text_ready.connect(self.console.append_text)
        self.stderr_stream.text_ready.connect(self.console.append_text)
        sys.stdout = self.stdout_stream  # type: ignore
        sys.stderr = self.stderr_stream  # type: ignore

        # Start decoder thread for display port
        self.decoder = DecoderThread(host, display_port, parent=self)
        self.decoder.frame_ready.connect(self.video_widget.set_frame)
        self.decoder.start()

        # Start TCP monitor threads for non-display ports
        self.stop_event = threading.Event()
        self.threads: list[threading.Thread] = []
        for p in ports:
            t = threading.Thread(target=tcp_listener, args=(self.stop_event, host, p, p == display_port), daemon=True)
            t.start()
            self.threads.append(t)

        # Start the forwarder (SERVER_IP:7002 -> 127.0.0.1:18087)
        fwd = threading.Thread(
            target=tcp_forwarder,
            args=(self.stop_event, host, FORWARD_SRC_PORT, FORWARD_DST_HOST, FORWARD_DST_PORT),
            daemon=True
        )
        fwd.start()
        self.threads.append(fwd)

        print(f"[{ts()}] UI ready. Close the window to quit.\n")

    def closeEvent(self, event: QtGui.QCloseEvent):
        try:
            if self.decoder.isRunning():
                self.decoder.stop()
                self.decoder.wait(2000)
            self.stop_event.set()
            for t in self.threads:
                t.join(timeout=1.0)
        except Exception:
            pass
        finally:
            sys.stdout = sys.__stdout__
            sys.stderr = sys.__stderr__
        super().closeEvent(event)

# ------------------ Entry -------------------
def main():
    # High-DPI friendliness
    QtWidgets.QApplication.setAttribute(QtCore.Qt.AA_UseHighDpiPixmaps, True)
    app = QtWidgets.QApplication(sys.argv)
    win = MainWindow(SERVER_IP, TCP_PORTS, DISPLAY_PORT)
    win.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
