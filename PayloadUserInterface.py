#!/usr/bin/env python3
"""
GUI H.264/MPEG-TS viewer with side-by-side console.

SIDE-B PORTS (as expected):
- HEARTBEAT (text)  UDP :6001
- VIDEO (mpegts)    UDP :7001
- COT (xml/text)    UDP :8001   ---> forwarded to TCP 127.0.0.1:18087

This version:
- STRIPS ALL UDP WAKEUP CODE
- DELETES THE SNIFFER
- PRINTS immediately when data is received on ANY port
- FIXES Windows PyAV UDP open errors (Errno 10014) by NOT using av.open("udp://...").
  Instead:
    - a Python UDP socket binds to :7001
    - received MPEG-TS datagrams are fed into PyAV via a file-like reader.
- NEW: Connects to TCP server at 127.0.0.1:18087 and forwards any datagrams
  arriving on COT_UDP_PORT to that TCP connection (with auto-reconnect).

Requires:
  pip install pyqt5 av opencv-python numpy
"""

import sys
import socket
import threading
import time
from collections import deque
from datetime import datetime
from typing import Optional

# --- Third-party ---
import av
import numpy as np
import cv2
from PyQt5 import QtCore, QtGui, QtWidgets

# ------------------ Config ------------------
# Side-B ports
HEARTBEAT_UDP_PORT = 6001   # status text
VIDEO_UDP_PORT     = 7001   # H.264 in MPEG-TS over UDP
COT_UDP_PORT       = 8001   # Cursor-on-Target XML (or any text)

# Bind host (0.0.0.0 to listen on all interfaces)
UDP_LISTEN_HOST = "192.168.43.74"

# TCP target for forwarding COT
COT_TCP_HOST = "127.0.0.1"
COT_TCP_PORT = 18087

# UDP buffer
UDP_MAX_DGRAM = 65535

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


# ---------- Qt console redirection (optional) ----------
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


# ---------- Video UDP -> file-like reader ----------
class UDPBytePipe:
    """
    Thread-safe byte buffer that provides a blocking .read(n) method,
    so PyAV/FFmpeg can read MPEG-TS bytes from it like a file.
    """
    def __init__(self, stop_event: threading.Event):
        self._buf = bytearray()
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._stop_event = stop_event
        self._closed = False

    def close(self):
        with self._cv:
            self._closed = True
            self._cv.notify_all()

    def push(self, data: bytes):
        if not data:
            return
        with self._cv:
            self._buf.extend(data)
            self._cv.notify_all()

    def readable(self):
        return True

    def read(self, n: int) -> bytes:
        with self._cv:
            while not self._stop_event.is_set() and not self._closed and len(self._buf) == 0:
                self._cv.wait(timeout=0.5)

            if self._stop_event.is_set() or self._closed:
                return b""

            take = min(max(1, int(n)), len(self._buf))
            out = bytes(self._buf[:take])
            del self._buf[:take]
            return out


def udp_video_receiver(stop_event: threading.Event, pipe: UDPBytePipe, listen_host: str, listen_port: int):
    """
    Binds UDP :7001 and pushes MPEG-TS datagrams into pipe for PyAV.
    Also prints immediately when datagrams arrive.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        except Exception:
            pass
        sock.bind((listen_host, int(listen_port)))
    except Exception as e:
        print(f"[{ts()}] [VIDEO] Bind failed on {listen_host}:{listen_port}: {e}")
        try:
            sock.close()
        except Exception:
            pass
        pipe.close()
        return

    print(f"[{ts()}] [VIDEO] Listening on {listen_host}:{listen_port} (raw UDP). Feeding decoder...")

    sock.settimeout(0.5)
    first = True
    try:
        while not stop_event.is_set():
            try:
                data, peer = sock.recvfrom(UDP_MAX_DGRAM)
            except socket.timeout:
                continue
            except Exception as e:
                if stop_event.is_set():
                    break
                print(f"[{ts()}] [VIDEO] recv error: {e}")
                break

            if not data:
                continue

            if first:
                first = False
                print(f"[{ts()}] [VIDEO] <<< first datagram {len(data)}B from {peer[0]}:{peer[1]}")
            # else:
            #     print(f"[{ts()}] [VIDEO] <<< {len(data)}B from {peer[0]}:{peer[1]}")

            pipe.push(data)
    finally:
        try:
            sock.close()
        except Exception:
            pass
        pipe.close()
        print(f"[{ts()}] [VIDEO] Stopped.")


# --------- Decoder thread (PyAV) ------------
class DecoderThread(QtCore.QThread):
    frame_ready = QtCore.pyqtSignal(np.ndarray)  # emits BGR image

    def __init__(self, pipe: UDPBytePipe, parent=None, label: str = ""):
        super().__init__(parent)
        self.pipe = pipe
        self.label = label
        self._stop = threading.Event()

        self._ts_window = deque(maxlen=90)
        self._nominal_fps: Optional[float] = None
        self._width: Optional[int] = None
        self._height: Optional[int] = None
        self._codec: str = "unknown"

        self._rx_packets = 0
        self._rx_bytes = 0
        self._rx_last_log = time.time()
        self._rx_first_seen = False
        self._rx_last_size = 0

    def stop(self):
        self._stop.set()

    def _log_rx_if_needed(self, packet_size: int):
        now = time.time()
        self._rx_packets += 1
        self._rx_bytes += int(packet_size)
        self._rx_last_size = int(packet_size)

        if not self._rx_first_seen:
            self._rx_first_seen = True
            print(f"[{ts()}] [DECODE] first demuxed packet (size={self._rx_last_size}B)")

        dt = now - self._rx_last_log
        if dt >= 1.0:
            pps = self._rx_packets / dt
            bps = self._rx_bytes / dt
            kbps = (bps * 8.0) / 1000.0
            print(f"[{ts()}] [DECODE] {pps:.1f} packets/s, {kbps:.1f} kbps, last={self._rx_last_size}B")
            self._rx_packets = 0
            self._rx_bytes = 0
            self._rx_last_log = now

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
        label_text = self.label or "MPEG-TS via UDPBytePipe"

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
        print(f"[{ts()}] [DECODE] Opening PyAV container from UDPBytePipe (mpegts, h264)...")
        try:
            container = av.open(self.pipe, format="mpegts")
        except Exception as e:
            print(f"[{ts()}] [DECODE] Failed to open from pipe: {e}")
            return

        video_stream = next((s for s in container.streams if s.type == "video"), None)
        if not video_stream:
            print(f"[{ts()}] [DECODE] No video stream found.")
            try:
                container.close()
            except Exception:
                pass
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

                try:
                    self._log_rx_if_needed(packet.size or 0)
                except Exception:
                    pass

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


# ---------- Straight UDP text listeners ----------
def udp_text_listener(stop_event: threading.Event, label: str, listen_port: int):
    """
    Binds UDP :listen_port and prints received datagrams (safe preview).
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        except Exception:
            pass
        sock.bind((UDP_LISTEN_HOST, int(listen_port)))
    except Exception as e:
        print(f"[{ts()}] [{label}] Bind failed on {UDP_LISTEN_HOST}:{listen_port}: {e}")
        try:
            sock.close()
        except Exception:
            pass
        return

    print(f"[{ts()}] [{label}] Listening on {UDP_LISTEN_HOST}:{listen_port}")
    sock.settimeout(0.5)

    try:
        while not stop_event.is_set():
            try:
                data, peer = sock.recvfrom(UDP_MAX_DGRAM)
            except socket.timeout:
                continue
            except Exception as e:
                if stop_event.is_set():
                    break
                print(f"[{ts()}] [{label}] recv error: {e}")
                break

            if not data:
                continue

            print(f"[{ts()}] [{label}] <<< {peer[0]}:{peer[1]}  {len(data)}B [{safe_preview(data)}]")
    finally:
        try:
            sock.close()
        except Exception:
            pass
        print(f"[{ts()}] [{label}] Stopped.")


# ---------- UDP COT -> TCP forwarder ----------
def udp_to_tcp_forwarder(
    stop_event: threading.Event,
    label: str,
    udp_host: str,
    udp_port: int,
    tcp_host: str,
    tcp_port: int,
):
    """
    Listen on UDP (udp_host:udp_port). For every datagram:
      - print immediately
      - forward raw bytes to a TCP server (tcp_host:tcp_port) via sendall()
    Auto-reconnects TCP on failure.
    """
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        try:
            udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        except Exception:
            pass
        udp_sock.bind((udp_host, int(udp_port)))
    except Exception as e:
        print(f"[{ts()}] [{label}] UDP bind failed on {udp_host}:{udp_port}: {e}")
        try:
            udp_sock.close()
        except Exception:
            pass
        return

    udp_sock.settimeout(0.5)
    print(f"[{ts()}] [{label}] Listening on UDP {udp_host}:{udp_port} and forwarding to TCP {tcp_host}:{tcp_port}")

    tcp_sock: Optional[socket.socket] = None
    last_connect_log = 0.0

    def tcp_close():
        nonlocal tcp_sock
        if tcp_sock is not None:
            try:
                tcp_sock.close()
            except Exception:
                pass
        tcp_sock = None

    def tcp_ensure_connected() -> bool:
        nonlocal tcp_sock, last_connect_log
        if tcp_sock is not None:
            return True

        now = time.time()
        # avoid spamming logs if server is down
        if now - last_connect_log > 1.0:
            print(f"[{ts()}] [{label}] TCP connecting to {tcp_host}:{tcp_port}...")
            last_connect_log = now

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2.0)
        try:
            s.connect((tcp_host, int(tcp_port)))
        except Exception as e:
            try:
                s.close()
            except Exception:
                pass
            # Keep trying in the main loop
            return False

        # Connected
        try:
            s.settimeout(None)  # blocking for sendall
        except Exception:
            pass

        tcp_sock = s
        print(f"[{ts()}] [{label}] TCP connected to {tcp_host}:{tcp_port}")
        return True

    try:
        while not stop_event.is_set():
            try:
                data, peer = udp_sock.recvfrom(UDP_MAX_DGRAM)
            except socket.timeout:
                continue
            except Exception as e:
                if stop_event.is_set():
                    break
                print(f"[{ts()}] [{label}] UDP recv error: {e}")
                break

            if not data:
                continue

            # Print immediately on receive
            print(f"[{ts()}] [{label}] <<< {peer[0]}:{peer[1]}  {len(data)}B [{safe_preview(data)}]")

            # Forward to TCP
            if not tcp_ensure_connected():
                # No TCP yet; drop this packet (or you could buffer if you prefer)
                print(f"[{ts()}] [{label}] TCP not connected; dropped {len(data)}B")
                continue

            try:
                tcp_sock.sendall(data)  # type: ignore[union-attr]
            except Exception as e:
                print(f"[{ts()}] [{label}] TCP send failed ({e}); reconnecting...")
                tcp_close()
                # Try once more immediately (optional)
                if tcp_ensure_connected():
                    try:
                        tcp_sock.sendall(data)  # type: ignore[union-attr]
                    except Exception as e2:
                        print(f"[{ts()}] [{label}] TCP re-send failed ({e2}); dropped {len(data)}B")
                        tcp_close()
                else:
                    print(f"[{ts()}] [{label}] TCP reconnect failed; dropped {len(data)}B")

    finally:
        tcp_close()
        try:
            udp_sock.close()
        except Exception:
            pass
        print(f"[{ts()}] [{label}] Stopped.")


# -------------- Main window ----------------
class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(WINDOW_TITLE)
        self.resize(INITIAL_WIN_W, INITIAL_WIN_H)

        central = QtWidgets.QWidget(self)
        outer_vbox = QtWidgets.QVBoxLayout(central)
        outer_vbox.setContentsMargins(8, 8, 8, 8)
        outer_vbox.setSpacing(8)

        header = QtWidgets.QLabel(
            f"<b>Listening (side-B):</b> "
            f"HB UDP {UDP_LISTEN_HOST}:{HEARTBEAT_UDP_PORT} | "
            f"Video UDP {UDP_LISTEN_HOST}:{VIDEO_UDP_PORT} | "
            f"CoT UDP {UDP_LISTEN_HOST}:{COT_UDP_PORT} "
            f"&rarr; TCP {COT_TCP_HOST}:{COT_TCP_PORT}"
        )
        outer_vbox.addWidget(header)

        splitter = QtWidgets.QSplitter(QtCore.Qt.Horizontal)

        video_group = QtWidgets.QGroupBox("Video (H.264 / MPEG-TS)")
        video_layout = QtWidgets.QVBoxLayout(video_group)
        self.video_widget = VideoWidget()
        video_layout.addWidget(self.video_widget)
        splitter.addWidget(video_group)

        console_group = QtWidgets.QGroupBox("Console (stdout / stderr)")
        console_layout = QtWidgets.QVBoxLayout(console_group)
        self.console = ConsoleWidget()
        console_layout.addWidget(self.console)
        splitter.addWidget(console_group)

        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 2)
        splitter.setSizes([int(INITIAL_WIN_W * 0.65), int(INITIAL_WIN_W * 0.35)])

        outer_vbox.addWidget(splitter, stretch=1)
        self.setCentralWidget(central)

        # Threads + stop
        self.stop_event = threading.Event()
        self.threads: list[threading.Thread] = []

        # --- Video pipeline: UDP receiver -> pipe -> decoder ---
        self.video_pipe = UDPBytePipe(self.stop_event)

        vt = threading.Thread(
            target=udp_video_receiver,
            args=(self.stop_event, self.video_pipe, UDP_LISTEN_HOST, VIDEO_UDP_PORT),
            daemon=True
        )
        vt.start()
        self.threads.append(vt)

        # Redirect stdout/stderr
        self.stdout_stream = EmittingStream()
        self.stderr_stream = EmittingStream()
        self.stdout_stream.text_ready.connect(self.console.append_text)
        self.stderr_stream.text_ready.connect(self.console.append_text)
        sys.stdout = self.stdout_stream  # type: ignore
        sys.stderr = self.stderr_stream  # type: ignore

        self.decoder = DecoderThread(
            self.video_pipe,
            parent=self,
            label=f"MPEG-TS over UDP (bound {UDP_LISTEN_HOST}:{VIDEO_UDP_PORT})",
        )
        self.decoder.frame_ready.connect(self.video_widget.set_frame)
        self.decoder.start()

        # --- HEARTBEAT listener on :6001 ---
        hb_t = threading.Thread(
            target=udp_text_listener,
            args=(self.stop_event, "HEARTBEAT", HEARTBEAT_UDP_PORT),
            daemon=True
        )
        hb_t.start()
        self.threads.append(hb_t)

        # --- COT forwarder: UDP :8001 -> TCP 127.0.0.1:18087 ---
        cot_t = threading.Thread(
            target=udp_to_tcp_forwarder,
            args=(self.stop_event, "COT", UDP_LISTEN_HOST, COT_UDP_PORT, COT_TCP_HOST, COT_TCP_PORT),
            daemon=True
        )
        cot_t.start()
        self.threads.append(cot_t)

        print(f"[{ts()}] UI ready. Close the window to quit.\n")

    def closeEvent(self, event: QtGui.QCloseEvent):
        try:
            self.stop_event.set()
            try:
                self.video_pipe.close()
            except Exception:
                pass

            if self.decoder.isRunning():
                self.decoder.stop()
                self.decoder.wait(2000)

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
    QtWidgets.QApplication.setAttribute(QtCore.Qt.AA_UseHighDpiPixmaps, True)
    app = QtWidgets.QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
