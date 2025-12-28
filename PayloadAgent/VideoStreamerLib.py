#!/usr/bin/env python3
"""
VideoStreamer.py

OpenCV-based “virtual gimbal” for a fisheye camera, with live pan/tilt/zoom control,
then re-encode and send out as MPEG-TS over UDP.

Compatible with older callers that still pass input_format=...
"""

from __future__ import annotations

import json
import shlex
import shutil
import socket
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional, Tuple, Union

import cv2
import numpy as np


# ----------------- helpers -----------------

def ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def clamp(v: float, lo: float, hi: float) -> float:
    return lo if v < lo else hi if v > hi else v


def safe_preview(b: bytes, n: int = 140) -> str:
    try:
        return b[:n].decode("utf-8", errors="replace").replace("\n", "\\n")
    except Exception:
        return repr(b[:n])


# ----------------- control state -----------------

@dataclass
class VideoControl:
    pan: float = 0.0
    tilt: float = 0.0
    zoom: float = 0.0
    auto: bool = False

    def clamped(self) -> "VideoControl":
        return VideoControl(
            pan=clamp(self.pan, -1.0, 1.0),
            tilt=clamp(self.tilt, -1.0, 1.0),
            zoom=clamp(self.zoom, -1.0, 1.0),
            auto=bool(self.auto),
        )


def decode_video_cmd(payload: Union[bytes, str]) -> VideoControl:
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8", errors="replace")

    try:
        d = json.loads(payload)
    except Exception:
        return VideoControl()

    return VideoControl(
        pan=float(d.get("pan", 0.0)),
        tilt=float(d.get("tilt", 0.0)),
        zoom=float(d.get("zoom", 0.0)),
        auto=bool(d.get("auto", False)),
    ).clamped()


# ----------------- streamer -----------------

class VideoStreamer:
    """
    Live virtual gimbal + encoder.
    """

    def __init__(
        self,
        *,
        video_port: int,
        ctrl_port: int,
        width: int,
        height: int,
        fps: int,
        bitrate_bps: int,
        v4l2_device: str,
        side_a_ip: str,

        # ⬇⬇⬇ Compatibility: accepted but unused ⬇⬇⬇
        input_format: Optional[str] = None,

        fisheye_fov_deg: float = 180.0,
        output_fov_deg: float = 45.0,
        output_size: Optional[Tuple[int, int]] = None,

        zoom_in_max: float = 0.75,
        zoom_out_max: float = 0.90,

        on_control: Optional[Callable[[bytes, Tuple[str, int]], None]] = None,
    ) -> None:
        self.video_port = int(video_port)
        self.ctrl_port = int(ctrl_port)
        self.width = int(width)
        self.height = int(height)
        self.fps = int(fps)
        self.bitrate_bps = int(bitrate_bps)
        self.v4l2_device = v4l2_device
        self.side_a_ip = side_a_ip
        self.input_format = input_format  # kept for compatibility
        self.on_control = on_control

        self.fisheye_fov_deg = float(fisheye_fov_deg)
        self.output_fov_deg = float(output_fov_deg)
        self.out_w, self.out_h = output_size or (self.width, self.height)

        self.zoom_in_max = zoom_in_max
        self.zoom_out_max = zoom_out_max

        self._stop = threading.Event()
        self._ctl = VideoControl()
        self._ctl_lock = threading.Lock()

        self._ctrl_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._ctrl_sock.bind(("0.0.0.0", self.ctrl_port))
        self._ctrl_sock.settimeout(0.5)

        self._rx_thread = None
        self._cap_thread = None

        self._cap = None
        self._ff = None

        self._frame_period = 1.0 / max(1, self.fps)

    # ---------- lifecycle ----------

    def start(self):
        self._rx_thread = threading.Thread(target=self._rx_loop, daemon=True)
        self._cap_thread = threading.Thread(target=self._capture_loop, daemon=True)
        self._rx_thread.start()
        self._cap_thread.start()
        print(f"[{ts()}] VideoStreamer started")

    def stop(self):
        self._stop.set()
        try:
            self._ctrl_sock.close()
        except Exception:
            pass
        self._stop_ffmpeg()
        if self._cap:
            self._cap.release()

    # ---------- control ----------

    def _rx_loop(self):
        print(f"[{ts()}] [VID-CTRL] Listening on UDP :{self.ctrl_port}")
        while not self._stop.is_set():
            try:
                data, addr = self._ctrl_sock.recvfrom(4096)
            except socket.timeout:
                continue
            except Exception:
                break

            ctl = decode_video_cmd(data)
            with self._ctl_lock:
                self._ctl = ctl

            print(f"[{ts()}] [VID-CTRL] {safe_preview(data)}")

            if self.on_control:
                self.on_control(data, addr)

    def _get_ctl(self) -> VideoControl:
        with self._ctl_lock:
            return self._ctl

    # ---------- capture / crop ----------

    def _compute_crop(self, zoom: float):
        base = self.output_fov_deg / self.fisheye_fov_deg
        base = clamp(base, 0.05, 1.0)

        if zoom >= 0:
            scale = base * (1.0 - self.zoom_in_max * zoom)
        else:
            scale = base * (1.0 + self.zoom_out_max * (-zoom))

        scale = clamp(scale, 0.05, 1.0)
        return int(self.width * scale), int(self.height * scale)

    def _crop_frame(self, frame, ctl: VideoControl):
        h, w = frame.shape[:2]
        cw, ch = self._compute_crop(ctl.zoom)

        max_off_x = max(0.0, 0.5 - cw / (2 * w))
        max_off_y = max(0.0, 0.5 - ch / (2 * h))

        cx = int((0.5 + ctl.pan * max_off_x) * w)
        cy = int((0.5 - ctl.tilt * max_off_y) * h)

        x1 = clamp(cx - cw // 2, 0, w - cw)
        y1 = clamp(cy - ch // 2, 0, h - ch)

        roi = frame[int(y1):int(y1 + ch), int(x1):int(x1 + cw)]
        return cv2.resize(roi, (self.out_w, self.out_h))

    # ---------- ffmpeg ----------

    def _have(self, c): return shutil.which(c) is not None

    def _ff_cmd(self, enc):
        url = f"udp://{self.side_a_ip}:{self.video_port}?pkt_size=1316"
        base = [
            "ffmpeg", "-f", "rawvideo", "-pix_fmt", "bgr24",
            "-s", f"{self.out_w}x{self.out_h}", "-r", str(self.fps),
            "-i", "pipe:0",
            "-an", "-f", "mpegts", url
        ]
        if enc == "hw":
            return ["nice", "-n", "10"] + base + ["-c:v", "h264_v4l2m2m"]
        return base + ["-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency"]

    def _start_ffmpeg(self):
        for enc in ("hw", "sw"):
            try:
                cmd = self._ff_cmd(enc)
                print("[FFMPEG]", " ".join(cmd))
                p = subprocess.Popen(cmd, stdin=subprocess.PIPE)
                time.sleep(0.3)
                if p.poll() is None:
                    self._ff = p
                    return
            except Exception:
                pass
        print("[FFMPEG] ERROR: no encoder")

    def _stop_ffmpeg(self):
        if self._ff:
            try:
                self._ff.terminate()
            except Exception:
                pass
            self._ff = None

    # ---------- main loop ----------

    def _capture_loop(self):
        self._cap = cv2.VideoCapture(self.v4l2_device)
        self._cap.set(cv2.CAP_PROP_FRAME_WIDTH, self.width)
        self._cap.set(cv2.CAP_PROP_FRAME_HEIGHT, self.height)

        self._start_ffmpeg()
        last = 0.0

        while not self._stop.is_set():
            if time.time() - last < self._frame_period:
                time.sleep(0.002)
                continue
            last = time.time()

            ok, frame = self._cap.read()
            if not ok:
                continue

            ctl = self._get_ctl()
            out = self._crop_frame(frame, ctl)

            try:
                self._ff.stdin.write(out.tobytes())
            except Exception:
                self._stop_ffmpeg()
                self._start_ffmpeg()

        self._stop_ffmpeg()
        self._cap.release()


# ------------------ standalone test ------------------

if __name__ == "__main__":
    vs = VideoStreamer(
        video_port=7001,
        ctrl_port=6001,
        width=1280,
        height=720,
        fps=8,
        bitrate_bps=2_000_000,
        v4l2_device="/dev/video0",
        side_a_ip="192.168.144.11",
        input_format="mjpeg",  # now accepted
    )

    try:
        vs.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        vs.stop()
