
# --------- Decoder thread (PyAV) ------------
from PyQt5 import QtCore
import av
import cv2
import time
import numpy as np
import threading
import socket
import json
from collections import deque

from UtilsLib import ts

class DecoderThread(QtCore.QThread):
    frame_ready = QtCore.pyqtSignal(np.ndarray)  # emits BGR image

    def __init__(self, pipe, parent, label):
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
            print(f"[{ts()}] [VIDEO] first demuxed packet (size={self._rx_last_size}B)")

        dt = now - self._rx_last_log
        if dt >= 1.0:
            pps = self._rx_packets / dt
            bps = self._rx_bytes / dt
            kbps = (bps * 8.0) / 1000.0
            print(f"[{ts()}] [VIDEO] {pps:.1f} packets/s, {kbps:.1f} kbps, last={self._rx_last_size}B")
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
        print(f"[{ts()}] [VIDEO] Opening PyAV container from UDPBytePipe (mpegts, h264)...")
        try:
            container = av.open(
                self.pipe,
                format="mpegts",
                options={
                    "fflags": "nobuffer",
                    #"flags": "low_delay",
                    #"probesize": "32",          # smaller => faster start, lower latency
                    "analyzeduration": "0",     # don't sit analyzing
                    "flush_packets": "1",
                }
            )
        except Exception as e:
            print(f"[{ts()}] [VIDEO] Failed to open from pipe: {e}")
            return

        video_stream = next((s for s in container.streams if s.type == "video"), None)
        if not video_stream:
            print(f"[{ts()}] [VIDEO] No video stream found.")
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

        print(f"[{ts()}] [VIDEO] Stream ready. Codec={self._codec}, nominal_fps={self._nominal_fps}")

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
            print(f"[{ts()}] [VIDEO] FFmpeg/AV exit: {e}")
        except Exception as e:
            print(f"[{ts()}] [VIDEO] Error: {e}")
        finally:
            try:
                container.close()
            except Exception:
                pass
            print(f"[{ts()}] [VIDEO] Stopped.")

class VideoUDPReceiver:
    """
    Owns:
      - UDP socket bind + recv loop
      - pushes received MPEG-TS datagrams into a UDPBytePipe
    """
    def __init__(
        self, stop_event, label, pipe,
        listen_host, listen_port,max_dgram):

        self.stop_event = stop_event
        self.pipe = pipe
        self.listen_host = listen_host
        self.listen_port = int(listen_port)
        self.max_dgram = int(max_dgram)
        self.label = label

        self._thread: Optional[threading.Thread] = None
        self._sock: Optional[socket.socket] = None
    # def

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
    # def

    def stop(self):
        # stop_event is managed by the app; just help unblock I/O
        try:
            if self._sock is not None:
                self._sock.close()
        except Exception:
            pass
    # def

    def join(self, timeout: float = 1.0):
        if self._thread:
            self._thread.join(timeout=timeout)
    # def

    def _run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock = sock
        try:
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            except Exception:
                pass

            sock.bind((self.listen_host, self.listen_port))
        except Exception as e:
            print(f"[{ts()}] [{self.label}] Bind failed on {self.listen_host}:{self.listen_port}: {e}")
            try:
                sock.close()
            except Exception:
                pass
            self.pipe.close()
            return

        print(f"[{ts()}] [{self.label}] Listening on {self.listen_host}:{self.listen_port} (raw UDP). Feeding decoder...")
        sock.settimeout(0.5)

        first = True
        try:
            while not self.stop_event.is_set():
                try:
                    data, peer = sock.recvfrom(self.max_dgram)
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.stop_event.is_set():
                        break
                    print(f"[{ts()}] [{self.label}] recv error: {e}")
                    break

                if not data:
                    continue

                if first:
                    first = False
                    print(f"[{ts()}] [{self.label}] <<< first datagram {len(data)}B from {peer[0]}:{peer[1]}")

                self.pipe.push(data)
        finally:
            try:
                sock.close()
            except Exception:
                pass
            self.pipe.close()
            print(f"[{ts()}] [{self.label}] Stopped.")
    # def

# class

class VideoCommandSender:
    """
    Owns:
      - UDP socket
      - periodic send loop
      - destination IP learned from PeerState (last status/heartbeat peer)

    Sends JSON payload:
      {"pan": x, "tilt": y, "zoom": zoom}
    All values are floats in [-1, 1].
    """
    def __init__(
        self,
        stop_event, label,
        payload_state,
        local_bind_host, dest_port, interval_s
    ):
        self.stop_event = stop_event
        self.payload_state = payload_state
        self.local_bind_host = local_bind_host
        self.dest_port = int(dest_port)
        self.interval_s = float(interval_s)
        self.label = label

        self._thread = None
        self._sock = None

        # --- stored command state (thread-safe) ---
        self._lock = threading.Lock()
        self._pan: float = 0.0
        self._tilt: float = 0.0
        self._zoom: float = 0.0
        self._auto: bool = False

        # Optional: if you ever want immediate sending on updates
        self._dirty_event = threading.Event()

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        try:
            if self._sock is not None:
                self._sock.close()
        except Exception:
            pass

    def join(self, timeout: float = 1.0):
        if self._thread:
            self._thread.join(timeout=timeout)

    @staticmethod
    def _clamp(v: float) -> float:
        try:
            v = float(v)
        except Exception:
            return 0.0
        if v < -1.0:
            return -1.0
        if v > 1.0:
            return 1.0
        return v

    def set_axes(self, x, y):
        """Store pan/tilt in [-1, 1]."""
        with self._lock:
            self._pan = self._clamp(x)
            self._tilt = self._clamp(y)

        # Optional: trigger immediate send rather than waiting interval
        # self._dirty_event.set()

    def set_zoom(self, z):
        """Store zoom in [-1, 1]."""
        with self._lock:
            self._zoom = self._clamp(z)

        # Optional: trigger immediate send rather than waiting interval
        # self._dirty_event.set()

    def set_auto(self, a):
        """Store auto as bool."""
        with self._lock:
            self._auto = bool(a)

        # Optional: trigger immediate send rather than waiting interval
        # self._dirty_event.set()

    def _get_state(self):
        with self._lock:
            return self._pan, self._tilt, self._zoom, self._auto

    def _run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock = sock

        try:
            sock.bind((self.local_bind_host, 0))
        except Exception:
            pass

        print(f"[{ts()}] [{self.label}] Sender enabled. "
              f"Will transmit JSON commands to UDP :{self.dest_port} (dest IP learned from status).")

        try:
            while not self.stop_event.is_set():
                ip = self.payload_state.get_hb_peer()
                if not ip:
                    if self.stop_event.wait(0.25):
                        break
                    continue

                pan, tilt, zoom, auto = self._get_state()
                msg = {"pan": pan, "tilt": tilt, "zoom": zoom, "auto": auto}
                payload = (json.dumps(msg, separators=(",", ":")) + "\n").encode("utf-8", errors="replace")

                try:
                    sock.sendto(payload, (ip, self.dest_port))
                    # print(f"[{ts()}] [{self.label}] >>>> {ip}:{self.dest_port}  " f"{len(payload)}B [{safe_preview(payload)}]")
                except Exception as e:
                    print(f"[{ts()}] [{self.label}] send error to {ip}:{self.dest_port}: {e}")

                # Periodic send. If you want "send immediately on change",
                # replace this wait with a wait on _dirty_event with timeout.
                if self.stop_event.wait(self.interval_s):
                    break

        finally:
            try:
                sock.close()
            except Exception:
                pass
            print(f"[{ts()}] [{self.label}] Stopped.")

    # def

# class

