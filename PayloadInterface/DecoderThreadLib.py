# --------- Decoder thread (PyAV) ------------
from PyQt5 import QtCore
import av
import cv2
import time
import numpy as np
import threading
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


