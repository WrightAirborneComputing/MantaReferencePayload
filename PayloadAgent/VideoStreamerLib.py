#!/usr/bin/env python3
"""
VideoStreamer.py

OpenCV-based “virtual gimbal” for a fisheye camera, with live pan/tilt/zoom control,
then software re-encode (libx264) and send out as MPEG-TS over UDP.

Incoming JSON example (UDP ctrl_port):
  {"pan":0.0,"tilt":-0.0,"zoom":0.0,"auto":false}

Notes:
- SW encoder only (libx264).
- Includes ensure_running() so existing supervisor code can keep calling it.
- Optional fisheye undistortion:
    Put your calibration K and D in FISHEYE_K and FISHEYE_D below.
- If auto==True, applies tilt compensation from vehicle pitch assuming 180° fisheye FOV (±90°).

Latency-focused changes (sender):
- OpenCV capture buffering reduced (CAP_PROP_BUFFERSIZE=1 where supported).
- ffmpeg flags for low latency: -fflags nobuffer, -flags low_delay, -flush_packets 1, -max_delay 0
- x264 tuned for low latency: zerolatency, no B-frames, short GOP, repeat headers, aud
- mpegts flags to push headers and minimize buffering
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

USE_FISHEYE_UNDISTORT = True

FISHEYE_K = np.array(
    [[600.0,   0.0, 640.0],
     [  0.0, 600.0, 360.0],
     [  0.0,   0.0,   1.0]],
    dtype=np.float32
)

FISHEYE_D = np.array([-0.05, 0.01, 0.0, 0.0], dtype=np.float32)


# ----------------- control state -----------------

@dataclass
class VideoControl:
    pan: float = 0.0   # [-1..+1]
    tilt: float = 0.0  # [-1..+1] (positive = UP)
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

    NOTE: tilt is inverted here (keeps your current behavior).
    If you want UI "up" to be +tilt, ensure your sender matches this convention.
    """
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8", errors="replace")

    try:
        d = json.loads(payload)
    except Exception:
        return VideoControl()

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
        output_size: Optional[Tuple[int, int]] = None,

        # virtual gimbal settings
        fisheye_fov_deg: float = 180.0,
        output_fov_deg: float = 45.0,

        # zoom response tuning
        zoom_in_max: float = 0.75,
        zoom_out_max: float = 0.90,

        # Optional callback on control packets
        on_control: Optional[Callable[[bytes, Tuple[str, int]], None]] = None,

        # restart shaping
        ffmpeg_restart_backoff_s: float = 0.6,

        # latency / encoding tuning
        gop_seconds: float = 1.0,           # keyframe interval in seconds (smaller = lower latency / faster recovery)
        x264_threads: int = 2,              # keep small on Pi
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

        self.gop_seconds = float(gop_seconds)
        self.x264_threads = int(x264_threads)

        # pitch filter for auto-tilt
        self.pitch_smoothing_alpha = 0.90
        self._pitch_filt_rad = 0.0
        self._pitch_filt_ready = False

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
            # keep this quiet-ish; supervisor may call often
            # print("[SUP] FFmpeg not running; starting...")
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

            # Uncomment for debugging:
            # print(f"[{ts()}] [VID-CTRL] <- {addr[0]}:{addr[1]} {safe_preview(data)}")

            if self.on_control:
                try:
                    self.on_control(data, addr)
                except Exception as e:
                    print(f"[{ts()}] [VID-CTRL] on_control error: {e}")

    def _get_ctl(self) -> VideoControl:
        with self._ctl_lock:
            return self._ctl

    # ---------- auto-tilt from pitch ----------

    def _get_pitch_rad_filtered(self) -> Optional[float]:
        """
        Returns filtered pitch in radians, or None if no pitch available.
        Requires MavlinkInterface to implement get_pitch_rad().
        """
        try:
            p = self.mavlink_interface.get_pitch_rad()
        except Exception:
            return None

        if p is None:
            return None

        a = clamp(self.pitch_smoothing_alpha, 0.0, 0.995)
        if not self._pitch_filt_ready:
            self._pitch_filt_rad = float(p)
            self._pitch_filt_ready = True
        else:
            self._pitch_filt_rad = (a * self._pitch_filt_rad) + ((1.0 - a) * float(p))

        return self._pitch_filt_rad

    def _apply_auto_tilt_if_enabled(self, ctl: VideoControl) -> VideoControl:
        """
        If ctl.auto is True, adjust tilt based on vehicle pitch.

        Assumption: total fisheye FOV = 180° => half-angle = 90°.
        Normalize pitch to [-1..+1] by dividing by 90°.
        Compensation sign:
          pitch up (+) => we want to look down => tilt becomes more NEGATIVE.
        """
        if not ctl.auto:
            return ctl

        pitch_rad = self._get_pitch_rad_filtered()
        if pitch_rad is None:
            return ctl

        pitch_deg = float(pitch_rad) * (180.0 / 3.141592653589793)
        half_fov = max(1e-6, self.fisheye_fov_deg / 2.0)  # should be 90 for 180 total
        pitch_frac = clamp(pitch_deg / half_fov, -1.0, 1.0)

        # gain 1.0 => 1:1 mapping of pitch_frac to tilt fraction
        auto_tilt_gain = 1.0
        tilt_eff = ctl.tilt - (auto_tilt_gain * pitch_frac)

        return VideoControl(
            pan=ctl.pan,
            tilt=clamp(tilt_eff, -1.0, 1.0),
            zoom=ctl.zoom,
            auto=ctl.auto,
        )

    # ---------- crop math ----------

    def _compute_crop_wh(self, zoom: float) -> Tuple[int, int]:
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

        max_off_x = max(0.0, 0.5 - cw / (2.0 * w))
        max_off_y = max(0.0, 0.5 - ch / (2.0 * h))

        cx = int((0.5 + ctl.pan * max_off_x) * w)
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
        # UDP output: keep small buffers, and ask ffmpeg to flush packets aggressively.
        url = f"udp://{self.side_a_ip}:{self.video_port}?pkt_size=1316&buffer_size=65536"

        # GOP / keyframes: shorter GOP can reduce end-to-end delay and improve recovery.
        gop = max(2, int(round(self.fps * max(0.25, self.gop_seconds))))

        # x264 params: no bframes, repeat headers, aud helps some decoders
        x264_params = f"bframes=0:ref=1:scenecut=0:repeat-headers=1:aud=1:keyint={gop}:min-keyint={gop}"

        cmd = [
            "ffmpeg",
            "-hide_banner", "-loglevel", "warning",

            # low-latency demux/processing behavior
            "-fflags", "nobuffer",
            "-flags", "low_delay",
            "-flush_packets", "1",
            "-max_delay", "0",

            # raw frames in
            "-f", "rawvideo",
            "-pix_fmt", "bgr24",
            "-s", f"{self.out_w}x{self.out_h}",
            "-r", str(self.fps),
            "-i", "pipe:0",

            "-an",

            # encoder
            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-tune", "zerolatency",
            "-threads", str(max(1, self.x264_threads)),
            "-pix_fmt", "yuv420p",
            "-b:v", str(self.bitrate_bps),
            "-maxrate", str(self.bitrate_bps),
            "-bufsize", str(max(1, self.bitrate_bps // 4)),  # smaller VBV buffer => lower latency (may risk quality)
            "-g", str(gop),
            "-keyint_min", str(gop),
            "-sc_threshold", "0",
            "-x264-params", x264_params,

            # mux
            "-f", "mpegts",
            "-muxdelay", "0",
            "-muxpreload", "0",
            "-mpegts_flags", "+resend_headers",
            url,
        ]

        prio = []
        if self._have("nice"):
            prio += ["nice", "-n", "10"]
        if self._have("ionice"):
            prio += ["ionice", "-c2", "-n", "7"]
        return prio + cmd

    def _start_ffmpeg(self) -> None:
        now = time.time()
        if (now - self._last_ffmpeg_start_ts) < self.ffmpeg_restart_backoff_s:
            return
        self._last_ffmpeg_start_ts = now

        self._stop_ffmpeg()

        try:
            cmd = self._ff_cmd_sw()
            print(f"[{ts()}] [FFMPEG] (SW) " + " ".join(cmd))
            p = subprocess.Popen(cmd, stdin=subprocess.PIPE, bufsize=0)
            time.sleep(0.2)
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

        # Minimize capture buffering if backend supports it.
        try:
            self._cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        except Exception:
            pass

        self._cap.set(cv2.CAP_PROP_FRAME_WIDTH, self.width)
        self._cap.set(cv2.CAP_PROP_FRAME_HEIGHT, self.height)

        self._start_ffmpeg()
        last_ts = 0.0

        while not self._stop.is_set():
            self.ensure_running()

            # throttle to fps
            if time.time() - last_ts < self._frame_period:
                time.sleep(0.001)
                continue
            last_ts = time.time()

            if self._cap is None:
                time.sleep(0.05)
                continue

            ok, frame = self._cap.read()
            if not ok or frame is None:
                time.sleep(0.005)
                continue

            h, w = frame.shape[:2]
            if USE_FISHEYE_UNDISTORT and not self._undist_ready:
                self._maybe_init_undistort(w, h)

            frame = self._undistort_if_enabled(frame)

            ctl = self._get_ctl()
            ctl = self._apply_auto_tilt_if_enabled(ctl)

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


# ------------------ standalone test ------------------

if __name__ == "__main__":
    # NOTE: this assumes you create and start MavlinkInterface elsewhere in your real app.
    # For a quick local run you can pass a stub with get_pitch_rad() returning None.

    class _StubMav:
        def get_pitch_rad(self):
            return None

    vs = VideoStreamer(
        video_port=7001,
        ctrl_port=6001,
        width=1280,
        height=720,
        fps=8,
        bitrate_bps=2_000_000,
        v4l2_device="/dev/video0",
        side_a_ip="192.168.144.11",
        mavlink_iface=_StubMav(),     # replace with real MavlinkInterface in PayloadAgent
        input_format="mjpeg",         # accepted, unused
        output_size=(1280, 720),

        fisheye_fov_deg=180.0,
        output_fov_deg=45.0,

        # latency knobs (adjust if needed)
        gop_seconds=0.8,
        x264_threads=2,
    )

    try:
        vs.start()
        while True:
            time.sleep(1.0)
            vs.ensure_running()
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        vs.stop()
