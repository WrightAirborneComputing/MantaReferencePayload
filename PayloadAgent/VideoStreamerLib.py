#!/usr/bin/env python3
"""
VideoStreamer.py

OpenCV-based “virtual gimbal” for a fisheye camera, with live pan/tilt/zoom control,
then software re-encode (libx264) and send out as MPEG-TS over UDP.

Incoming JSON example (UDP ctrl_port):
  {"pan":0.0,"tilt":-0.0,"zoom":0.0,"auto":false}

Notes:
- This version strips out ALL hardware encoding attempts (no h264_v4l2m2m).
- Includes ensure_running() so existing supervisor code can keep calling it.
- Optional fisheye undistortion:
    Put your calibration K and D in FISHEYE_K and FISHEYE_D below (see section).
"""

from __future__ import annotations

import json
import shutil
import socket
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional, Tuple, Union

import cv2
import numpy as np

from MavlinkInterfaceLib import MavlinkInterface

# ----------------- helpers -----------------

def ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def clamp(v: float, lo: float, hi: float) -> float:
    return lo if v < lo else hi if v > hi else v


def safe_preview(b: bytes, n: int = 160) -> str:
    try:
        return b[:n].decode("utf-8", errors="replace").replace("\n", "\\n")
    except Exception:
        return repr(b[:n])


# ----------------- fisheye calibration (PUT K & D HERE) -----------------
# If you haven't calibrated yet, leave USE_FISHEYE_UNDISTORT=False.
# Once you have calibration results, set USE_FISHEYE_UNDISTORT=True
# and paste your K and D values here.

USE_FISHEYE_UNDISTORT = True

# K: 3x3 camera matrix (np.float32)
FISHEYE_K = np.array(
    [[600.0,   0.0, 640.0],
     [  0.0, 600.0, 360.0],
     [  0.0,   0.0,   1.0]],
    dtype=np.float32
)

# D: fisheye distortion coefficients (k1,k2,k3,k4) (np.float32)
FISHEYE_D = np.array([-0.05, 0.01, 0.0, 0.0], dtype=np.float32)


# ----------------- control state -----------------

@dataclass
class VideoControl:
    pan: float = 0.0   # [-1..+1]
    tilt: float = 0.0  # [-1..+1] (positive = UP in your UI convention)
    zoom: float = 0.0  # [-1..+1]
    auto: bool = False

    def clamped(self) -> "VideoControl":
        return VideoControl(
            pan=clamp(self.pan, -1.0, 1.0),
            tilt=clamp(self.tilt, -1.0, 1.0),
            zoom=clamp(self.zoom, -1.0, 1.0),
            auto=bool(self.auto),
        )


def decode_video_cmd(payload: Union[bytes, str]) -> VideoControl:
    """
    Decodes {"pan":..,"tilt":..,"zoom":..,"auto":..} to VideoControl.
    Returns safe defaults on parse failure.
    """
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8", errors="replace")

    try:
        d = json.loads(payload)
    except Exception:
        return VideoControl()

    # Keep signs "as sent" by the UI:
    # - If your UI already inverts tilt so up => +, then don't invert here.
    # - Your UI earlier sent tilt as-is, so we keep it as-is.
    return VideoControl(
        pan=float(d.get("pan", 0.0)),
        tilt=-float(d.get("tilt", 0.0)),
        zoom=float(d.get("zoom", 0.0)),
        auto=bool(d.get("auto", False)),
    ).clamped()


# ----------------- streamer -----------------

class VideoStreamer:
    """
    Live virtual gimbal + SW encoder (libx264) -> MPEG-TS over UDP.
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
        mavlink_iface: MavlinkInterface,

        # compatibility arg (some callers still pass it)
        input_format: Optional[str] = None,

        fisheye_fov_deg: float = 180.0,
        output_fov_deg: float = 45.0,
        output_size: Optional[Tuple[int, int]] = None,

        # zoom response tuning
        zoom_in_max: float = 0.75,
        zoom_out_max: float = 0.90,

        # Optional callback on control packets
        on_control: Optional[Callable[[bytes, Tuple[str, int]], None]] = None,

        # Supervisor restart shaping
        ffmpeg_restart_backoff_s: float = 0.5,
    ) -> None:
        self.video_port = int(video_port)
        self.ctrl_port = int(ctrl_port)
        self.width = int(width)
        self.height = int(height)
        self.fps = int(fps)
        self.bitrate_bps = int(bitrate_bps)
        self.v4l2_device = v4l2_device
        self.side_a_ip = side_a_ip
        self.input_format = input_format  # unused but accepted
        self.on_control = on_control
        self.mavlink_interface = mavlink_iface

        self.fisheye_fov_deg = float(fisheye_fov_deg)
        self.output_fov_deg = float(output_fov_deg)
        self.out_w, self.out_h = output_size or (self.width, self.height)

        self.zoom_in_max = float(zoom_in_max)
        self.zoom_out_max = float(zoom_out_max)

        self.ffmpeg_restart_backoff_s = float(ffmpeg_restart_backoff_s)

        self._stop = threading.Event()
        self._ctl = VideoControl()
        self._ctl_lock = threading.Lock()

        self._ctrl_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._ctrl_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._ctrl_sock.bind(("0.0.0.0", self.ctrl_port))
        self._ctrl_sock.settimeout(0.5)

        self._rx_thread: Optional[threading.Thread] = None
        self._cap_thread: Optional[threading.Thread] = None

        self._cap: Optional[cv2.VideoCapture] = None
        self._ff: Optional[subprocess.Popen] = None

        self._frame_period = 1.0 / max(1, self.fps)

        # Undistort maps (computed once we know frame size)
        self._undist_map1 = None
        self._undist_map2 = None
        self._undist_ready = False

        # track last ffmpeg start time (for backoff)
        self._last_ffmpeg_start_ts = 0.0

    # ---------- lifecycle ----------

    def start(self) -> None:
        self._stop.clear()

        self._rx_thread = threading.Thread(target=self._rx_loop, name="VidCtrlRx", daemon=True)
        self._cap_thread = threading.Thread(target=self._capture_loop, name="VidCapture", daemon=True)

        self._rx_thread.start()
        self._cap_thread.start()

        print(f"[{ts()}] VideoStreamer started (CTRL UDP :{self.ctrl_port} -> stream UDP {self.side_a_ip}:{self.video_port}).")

    def stop(self) -> None:
        self._stop.set()

        try:
            self._ctrl_sock.close()
        except Exception:
            pass

        self._stop_ffmpeg()

        if self._cap is not None:
            try:
                self._cap.release()
            except Exception:
                pass
            self._cap = None

        if self._rx_thread:
            self._rx_thread.join(timeout=1.0)
        if self._cap_thread:
            self._cap_thread.join(timeout=1.0)

        print(f"[{ts()}] VideoStreamer stopped.")

    def ensure_running(self) -> None:
        """
        Kept for compatibility with your supervisor code.
        Ensures ffmpeg is up (SW encoder only).
        """
        if self._stop.is_set():
            return
        if self._ff is None or self._ff.poll() is not None:
            print("[SUP] FFmpeg not running; starting...")
            self._start_ffmpeg()

    # ---------- control ----------

    def _rx_loop(self) -> None:
        print(f"[{ts()}] [VID-CTRL] Listening on UDP 0.0.0.0:{self.ctrl_port}")
        while not self._stop.is_set():
            try:
                data, addr = self._ctrl_sock.recvfrom(4096)
            except socket.timeout:
                continue
            except Exception:
                break

            if not data:
                continue

            ctl = decode_video_cmd(data)
            with self._ctl_lock:
                self._ctl = ctl

            # print(f"[{ts()}] [VID-CTRL] <- {addr[0]}:{addr[1]} {safe_preview(data)}")

            if self.on_control:
                try:
                    self.on_control(data, addr)
                except Exception as e:
                    print(f"[{ts()}] [VID-CTRL] on_control error: {e}")

    def _get_ctl(self) -> VideoControl:
        with self._ctl_lock:
            return self._ctl

    # ---------- crop math ----------

    def _compute_crop_wh(self, zoom: float) -> Tuple[int, int]:
        """
        Compute crop width/height in pixels based on:
          base scale = output_fov / fisheye_fov
          zoom in => smaller crop, zoom out => bigger crop
        """
        base = self.output_fov_deg / self.fisheye_fov_deg
        base = clamp(base, 0.02, 1.0)

        if zoom >= 0.0:
            scale = base * (1.0 - self.zoom_in_max * zoom)
        else:
            scale = base * (1.0 + self.zoom_out_max * (-zoom))

        scale = clamp(scale, 0.02, 1.0)
        cw = max(8, int(self.width * scale))
        ch = max(8, int(self.height * scale))
        return cw, ch

    def _crop_frame(self, frame: np.ndarray, ctl: VideoControl) -> np.ndarray:
        """
        Pan/tilt tries to access as much of the fisheye image as possible.
        The maximum pan/tilt is limited only by keeping the crop inside the frame.
        """
        h, w = frame.shape[:2]
        cw, ch = self._compute_crop_wh(ctl.zoom)

        # how far can centre move (in fraction) before crop hits border?
        max_off_x = max(0.0, 0.5 - cw / (2.0 * w))
        max_off_y = max(0.0, 0.5 - ch / (2.0 * h))

        # Full range mapping: pan/tilt = +/-1 maps to full allowable offset
        cx = int((0.5 + ctl.pan * max_off_x) * w)
        # tilt +1 = up => smaller y
        cy = int((0.5 - ctl.tilt * max_off_y) * h)

        x1 = int(clamp(cx - cw // 2, 0, w - cw))
        y1 = int(clamp(cy - ch // 2, 0, h - ch))

        roi = frame[y1:y1 + ch, x1:x1 + cw]
        if roi.shape[0] != self.out_h or roi.shape[1] != self.out_w:
            roi = cv2.resize(roi, (self.out_w, self.out_h), interpolation=cv2.INTER_LINEAR)
        return roi

    # ---------- undistortion ----------

    def _maybe_init_undistort(self, frame_w: int, frame_h: int) -> None:
        if not USE_FISHEYE_UNDISTORT or self._undist_ready:
            return
        try:
            K = FISHEYE_K.astype(np.float32).copy()
            D = FISHEYE_D.astype(np.float32).reshape(4, 1).copy()
            R = np.eye(3, dtype=np.float32)

            # You can tweak the newK / balance later; start with same K.
            newK = K.copy()

            self._undist_map1, self._undist_map2 = cv2.fisheye.initUndistortRectifyMap(
                K, D, R, newK, (frame_w, frame_h), cv2.CV_16SC2
            )
            self._undist_ready = True
            print(f"[{ts()}] [UNDIST] Fisheye undistort maps ready for {frame_w}x{frame_h}.")
        except Exception as e:
            print(f"[{ts()}] [UNDIST] Failed to init undistort maps: {e}")
            self._undist_ready = False

    def _undistort_if_enabled(self, frame: np.ndarray) -> np.ndarray:
        if not USE_FISHEYE_UNDISTORT or not self._undist_ready:
            return frame
        try:
            return cv2.remap(
                frame,
                self._undist_map1,
                self._undist_map2,
                interpolation=cv2.INTER_LINEAR,
                borderMode=cv2.BORDER_CONSTANT,
            )
        except Exception:
            return frame

    # ---------- ffmpeg (SW only) ----------

    @staticmethod
    def _have(cmd: str) -> bool:
        return shutil.which(cmd) is not None

    def _ff_cmd_sw(self) -> list[str]:
        url = f"udp://{self.side_a_ip}:{self.video_port}?pkt_size=1316&buffer_size=1048576"

        # Important: put encoding args *before* the output URL.
        cmd = [
            "ffmpeg",
            "-hide_banner", "-loglevel", "warning",
            "-f", "rawvideo",
            "-pix_fmt", "bgr24",
            "-s", f"{self.out_w}x{self.out_h}",
            "-r", str(self.fps),
            "-i", "pipe:0",
            "-an",
            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-tune", "zerolatency",
            "-pix_fmt", "yuv420p",
            "-b:v", str(self.bitrate_bps),
            "-maxrate", str(self.bitrate_bps),
            "-bufsize", str(max(1, self.bitrate_bps // 2)),
            "-g", str(max(2, self.fps) * 2),
            "-keyint_min", str(max(2, self.fps) * 2),
            "-sc_threshold", "0",
            "-f", "mpegts",
            "-muxdelay", "0",
            "-muxpreload", "0",
            url,
        ]

        prio = []
        if self._have("nice"):
            prio += ["nice", "-n", "10"]
        if self._have("ionice"):
            prio += ["ionice", "-c2", "-n", "7"]
        return prio + cmd

    def _start_ffmpeg(self) -> None:
        # Backoff so a supervisor loop doesn't spawn relentlessly
        now = time.time()
        if (now - self._last_ffmpeg_start_ts) < self.ffmpeg_restart_backoff_s:
            return
        self._last_ffmpeg_start_ts = now

        self._stop_ffmpeg()

        try:
            cmd = self._ff_cmd_sw()
            print(f"[{ts()}] [FFMPEG] (SW) " + " ".join(cmd))
            p = subprocess.Popen(cmd, stdin=subprocess.PIPE)
            time.sleep(0.25)
            if p.poll() is None:
                self._ff = p
                print(f"[{ts()}] [FFMPEG] Running (sw).")
                return
            else:
                print(f"[{ts()}] [FFMPEG] Exited immediately (code {p.returncode}).")
        except Exception as e:
            print(f"[{ts()}] [FFMPEG] Failed to start: {e}")

        self._ff = None

    def _stop_ffmpeg(self) -> None:
        p = self._ff
        self._ff = None
        if not p:
            return
        try:
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=1.5)
                except Exception:
                    p.kill()
        except Exception:
            pass

    # ---------- main loop ----------

    def _capture_loop(self) -> None:
        self._cap = cv2.VideoCapture(self.v4l2_device)
        self._cap.set(cv2.CAP_PROP_FRAME_WIDTH, self.width)
        self._cap.set(cv2.CAP_PROP_FRAME_HEIGHT, self.height)

        # Start ffmpeg once
        self._start_ffmpeg()

        last_ts = 0.0

        while not self._stop.is_set():
            # keep ffmpeg alive
            self.ensure_running()

            # throttle to fps
            if time.time() - last_ts < self._frame_period:
                time.sleep(0.002)
                continue
            last_ts = time.time()

            if self._cap is None:
                time.sleep(0.05)
                continue

            ok, frame = self._cap.read()
            if not ok or frame is None:
                time.sleep(0.01)
                continue

            h, w = frame.shape[:2]
            if USE_FISHEYE_UNDISTORT and not self._undist_ready:
                self._maybe_init_undistort(w, h)

            frame = self._undistort_if_enabled(frame)

            ctl = self._get_ctl()
            out = self._crop_frame(frame, ctl)

            p = self._ff
            if p is None or p.stdin is None:
                continue

            try:
                p.stdin.write(out.tobytes())
            except Exception as e:
                print(f"[{ts()}] [CAP] ffmpeg stdin broken pipe ({e}); restarting ffmpeg...")
                self._start_ffmpeg()

        self._stop_ffmpeg()
        if self._cap is not None:
            try:
                self._cap.release()
            except Exception:
                pass
            self._cap = None

