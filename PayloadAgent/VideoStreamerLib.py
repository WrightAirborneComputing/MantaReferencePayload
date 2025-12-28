#!/usr/bin/env python3
"""
VideoStreamer.py

Owns:
  - ffmpeg command build/launch/stop
  - local listener on UDP:CTRL for incoming control commands (JSON bytes)
  - optional fisheye ROI extraction using FFmpeg v360 (fisheye -> rectilinear)

Control JSON example (from UI):
  {"pan":0.0,"tilt":-0.0,"zoom":0.0,"auto":false}

Notes:
- This file is written so that calling ensure_running() will *not* crash your
  supervisor if ffmpeg (or an encoder) is missing/fails; it logs and retries later.
"""

from __future__ import annotations

from typing import Optional, Callable, Tuple, Union
import time
import threading
import socket
import shutil
import shlex
import subprocess
import math
import json


class VideoStreamer:
    """
    Owns:
      - ffmpeg command build/launch/stop
      - local listener on UDP:ctrl_port for incoming control commands
      - If fisheye_crop_enable=True, applies FFmpeg v360 to extract a rectilinear
        45° FOV window from a 180° fisheye input, centered on roi_center_px.
    """

    def __init__(
        self,
        video_port: int,
        ctrl_port: int,
        width: int,
        height: int,
        fps: int,
        bitrate_bps: int,
        v4l2_device: str,
        input_format: str,
        side_a_ip: str,
        on_control: Optional[Callable[[bytes, tuple[str, int]], None]] = None,
        # ---- fisheye/ROI parameters ----
        fisheye_crop_enable: bool = True,
        fisheye_in_hfov_deg: float = 180.0,
        fisheye_in_vfov_deg: float = 180.0,
        roi_hfov_deg: float = 45.0,
        roi_vfov_deg: float = 45.0,
        roi_center_px: Optional[Tuple[float, float]] = None,  # (cx, cy) in px; default=image center
        roi_out_size: Optional[Tuple[int, int]] = None,       # (w, h) output; default=input size
    ) -> None:
        self.side_a_ip = side_a_ip
        self.video_port = int(video_port)
        self.ctrl_port = int(ctrl_port)
        self.width = int(width)
        self.height = int(height)
        self.fps = int(fps)
        self.bitrate_bps = int(bitrate_bps)
        self.v4l2_device = v4l2_device
        self.input_format = input_format
        self.on_control = on_control

        # ROI state
        self.fisheye_crop_enable = bool(fisheye_crop_enable)
        self.fisheye_in_hfov_deg = float(fisheye_in_hfov_deg)
        self.fisheye_in_vfov_deg = float(fisheye_in_vfov_deg)
        self.roi_hfov_deg = float(roi_hfov_deg)
        self.roi_vfov_deg = float(roi_vfov_deg)
        self.roi_center_px = roi_center_px
        self.roi_out_size = roi_out_size
        self._roi_lock = threading.Lock()

        self._ff: Optional[subprocess.Popen] = None
        self._stop = threading.Event()

        # RX-only control socket (bound local :ctrl_port)
        self._ctrl_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self._ctrl_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        except Exception:
            pass
        self._ctrl_sock.bind(("0.0.0.0", self.ctrl_port))

        self._last_cmd: Optional[bytes] = None
        self._last_cmd_from: Optional[tuple[str, int]] = None
        self._restart_count = 0
        self._last_exit_code: Optional[int] = None
        self._encoder_in_use: str = "unknown"

        self._rx_thread: Optional[threading.Thread] = None

        # last decoded controls (for debugging / other logic)
        self._last_pan = 0.0
        self._last_tilt = 0.0
        self._last_zoom = 0.0
        self._last_auto = False
        self._ctrl_lock = threading.Lock()

    # ---------------- ROI setters ----------------
    def set_roi_center_px(self, cx: float, cy: float) -> None:
        """Update ROI center in input image pixel coords."""
        with self._roi_lock:
            self.roi_center_px = (float(cx), float(cy))

    def set_roi_fov_deg(self, hfov: float, vfov: Optional[float] = None) -> None:
        """Update ROI FOV in degrees."""
        with self._roi_lock:
            self.roi_hfov_deg = float(hfov)
            self.roi_vfov_deg = float(vfov if vfov is not None else hfov)

    def close(self) -> None:
        try:
            self._ctrl_sock.close()
        except Exception:
            pass

    # ---- camera helpers ----
    @staticmethod
    def _have_cmd(name: str) -> bool:
        return shutil.which(name) is not None

    @staticmethod
    def _run_cmd(cmd: list[str]) -> int:
        try:
            return subprocess.run(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            ).returncode
        except Exception:
            return 1

    def _try_set_camera_parm(self) -> None:
        if self._have_cmd("v4l2-ctl"):
            self._run_cmd(["v4l2-ctl", "-d", self.v4l2_device, "--set-parm", str(self.fps)])

    # ---------------- pixel -> yaw/pitch ----------------
    def _roi_center_to_yaw_pitch_deg(self) -> tuple[float, float]:
        """
        Convert roi_center_px (pixel coords) to approximate yaw/pitch (degrees)
        for v360 using an equidistant fisheye model.

        - yaw: +right
        - pitch: +up
        """
        with self._roi_lock:
            cxcy = self.roi_center_px

        cx0 = (self.width / 2.0)
        cy0 = (self.height / 2.0)
        cx, cy = (cxcy if cxcy is not None else (cx0, cy0))

        dx = float(cx) - cx0           # +right
        dy = float(cy) - cy0           # +down
        r = math.hypot(dx, dy)
        if r < 1e-9:
            return (0.0, 0.0)

        # radius based on inscribed circle
        r_max = min(self.width, self.height) / 2.0

        # equidistant fisheye: theta scales linearly with radius
        theta_max = (self.fisheye_in_hfov_deg / 2.0)
        theta = max(0.0, min(theta_max, (r / r_max) * theta_max))

        yaw = theta * (dx / r)
        pitch = -theta * (dy / r)      # dy down => pitch negative (look down)
        return (yaw, pitch)

    # ---------------- build v360 filter ----------------
    def _build_vf_filter(self, encoder: str) -> Optional[str]:
        if not self.fisheye_crop_enable:
            return None

        yaw, pitch = self._roi_center_to_yaw_pitch_deg()

        with self._roi_lock:
            ohfov = float(self.roi_hfov_deg)
            ovfov = float(self.roi_vfov_deg)
            ow, oh = (self.roi_out_size if self.roi_out_size is not None else (self.width, self.height))

        vf = (
            "v360="
            f"input=fisheye:output=rectilinear:"
            f"ih_fov={self.fisheye_in_hfov_deg}:iv_fov={self.fisheye_in_vfov_deg}:"
            f"h_fov={ohfov}:v_fov={ovfov}:"
            f"yaw={yaw}:pitch={pitch}:"
            f"w={int(ow)}:h={int(oh)}"
        )

        # ensure pixel format matches encoder expectations
        if encoder == "h264_v4l2m2m":
            vf += ",format=nv12"
        else:
            vf += ",format=yuv420p"

        return vf

    def _build_ffmpeg_cmd(self, encoder: str) -> list[str]:
        url = f"udp://{self.side_a_ip}:{self.video_port}?pkt_size=1316&buffer_size=1048576"

        v4l2_input = [
            "-f", "v4l2",
            "-input_format", self.input_format,
            "-video_size", f"{self.width}x{self.height}",
            "-framerate", str(self.fps),
            "-i", self.v4l2_device,
        ]

        vf = self._build_vf_filter(encoder)
        vf_args = (["-vf", vf] if vf else [])

        if encoder == "h264_v4l2m2m":
            enc = [
                *vf_args,
                "-c:v", "h264_v4l2m2m",
                "-b:v", str(self.bitrate_bps),
                "-maxrate", str(self.bitrate_bps),
                "-bufsize", str(max(1, self.bitrate_bps // 2)),
                "-g", str(max(2, self.fps) * 2),
                "-bf", "0",
            ]
        else:
            enc = [
                *vf_args,
                "-c:v", "libx264",
                "-preset", "ultrafast",
                "-tune", "zerolatency",
                "-threads", "2",
                "-b:v", str(self.bitrate_bps),
                "-maxrate", str(self.bitrate_bps),
                "-bufsize", str(max(1, self.bitrate_bps // 2)),
                "-g", str(max(2, self.fps) * 2),
            ]

        base = [
            "ffmpeg", "-hide_banner", "-loglevel", "warning",
            "-rtbufsize", "32M",
            "-thread_queue_size", "256",
            *v4l2_input,
            *enc,
            "-fps_mode", "cfr",
            "-r", str(self.fps),
            "-an",
            "-f", "mpegts",
            "-muxdelay", "0",
            "-muxpreload", "0",
            url,
        ]

        prio = (
            ["nice", "-n", "10", "ionice", "-c2", "-n", "7"]
            if self._have_cmd("nice") and self._have_cmd("ionice")
            else []
        )
        return prio + base

    # ---- lifecycle ----
    def start(self) -> None:
        if self._rx_thread and self._rx_thread.is_alive():
            return
        self._stop.clear()

        self._rx_thread = threading.Thread(target=self._rx_loop, name="VideoCtrlRx", daemon=True)
        self._rx_thread.start()

        self.ensure_running()

    def stop(self) -> None:
        self._stop.set()
        self.stop_ffmpeg()
        if self._rx_thread:
            self._rx_thread.join(timeout=1.0)

    def ensure_running(self) -> None:
        """
        Ensure ffmpeg is running. This function is safe to call repeatedly.
        It will not raise if ffmpeg launch fails; it logs and returns.
        """
        try:
            if self._ff is None or self._ff.poll() is not None:
                if self._ff is not None:
                    self._last_exit_code = self._ff.returncode
                    print(f"[SUP] FFmpeg died (code {self._ff.returncode}); will recover...")
                self._restart_count += 1
                self._launch_ffmpeg()
        except Exception as e:
            # IMPORTANT: don't crash supervisor
            print(f"[SUP] ensure_running() error: {e}")
            # ensure we don't keep a half-baked Popen around
            try:
                self.stop_ffmpeg()
            except Exception:
                pass

    def stop_ffmpeg(self) -> None:
        ff = self._ff
        self._ff = None
        if not ff:
            return
        try:
            if ff.poll() is None:
                ff.terminate()
                try:
                    ret = ff.wait(timeout=2)
                    print(f"[FFMPEG] exited with code {ret}")
                except Exception:
                    ff.kill()
        except Exception:
            pass

    def _launch_ffmpeg(self) -> None:
        """
        Try HW encoder first; fall back to libx264.
        Never raises FileNotFoundError outward (handled here).
        """
        self._try_set_camera_parm()

        cmd_hw = self._build_ffmpeg_cmd("h264_v4l2m2m")
        print("[FFMPEG] (HW) ", " ".join(shlex.quote(x) for x in cmd_hw))

        try:
            ff = subprocess.Popen(cmd_hw)
        except FileNotFoundError as e:
            # ffmpeg not installed / not in PATH
            print(f"[FFMPEG] Launch failed (ffmpeg missing?): {e}")
            self._ff = None
            self._encoder_in_use = "none"
            return
        except Exception as e:
            print(f"[FFMPEG] Launch failed (HW cmd): {e}")
            self._ff = None
            self._encoder_in_use = "none"
            return

        time.sleep(1.0)

        if ff.poll() is not None and ff.returncode != 0:
            print(f"[FFMPEG] Hardware encoder failed (code {ff.returncode}). Trying software libx264...")
            cmd_sw = self._build_ffmpeg_cmd("libx264")
            print("[FFMPEG] (SW) ", " ".join(shlex.quote(x) for x in cmd_sw))
            try:
                ff2 = subprocess.Popen(cmd_sw)
            except Exception as e:
                print(f"[FFMPEG] Launch failed (SW cmd): {e}")
                self._ff = None
                self._encoder_in_use = "none"
                return
            self._encoder_in_use = "libx264"
            self._ff = ff2
        else:
            self._encoder_in_use = "h264_v4l2m2m"
            self._ff = ff

    # ---------------- control decode ----------------
    @staticmethod
    def decode_video_cmd(payload: Union[bytes, str]) -> Tuple[float, float, float, bool]:
        """
        Decode a video control JSON payload.

        Input example:
          {"pan":0.0,"tilt":-0.0,"zoom":0.0,"auto":false}

        Returns:
          (pan, tilt, zoom, auto)
          where pan/tilt/zoom are floats in [-1.0, +1.0]
          and auto is a bool.
        """
        def clamp(v: float) -> float:
            if v < -1.0:
                return -1.0
            if v > 1.0:
                return 1.0
            return v

        if isinstance(payload, (bytes, bytearray)):
            try:
                payload_str = payload.decode("utf-8", errors="replace")
            except Exception:
                payload_str = ""
        else:
            payload_str = payload

        try:
            data = json.loads(payload_str)
        except Exception:
            return 0.0, 0.0, 0.0, False

        try:
            pan = clamp(float(data.get("pan", 0.0)))
        except Exception:
            pan = 0.0

        try:
            tilt = clamp(float(data.get("tilt", 0.0)))
        except Exception:
            tilt = 0.0

        try:
            zoom = clamp(float(data.get("zoom", 0.0)))
        except Exception:
            zoom = 0.0

        auto = bool(data.get("auto", False))
        return pan, tilt, zoom, auto

    # ---------------- control RX loop ----------------
    def _rx_loop(self) -> None:
        self._ctrl_sock.settimeout(0.5)
        print(f"[VID-CTRL] Listening for control on UDP 0.0.0.0:{self.ctrl_port} (RX-only; JSON bytes)")

        while not self._stop.is_set():
            try:
                data, addr = self._ctrl_sock.recvfrom(4096)
                if not data:
                    continue

                self._last_cmd = data
                self._last_cmd_from = addr

                preview = data[:160].decode("utf-8", errors="replace").replace("\n", "\\n")
                # print(f"[VID-CTRL] <- {addr[0]}:{addr[1]} bytes={len(data)} head='{preview}'")

                pan, tilt, zoom, auto = self.decode_video_cmd(data)

                with self._ctrl_lock:
                    self._last_pan = pan
                    self._last_tilt = tilt
                    self._last_zoom = zoom
                    self._last_auto = auto

                print(f"[VID-CTRL] decoded pan={pan:+0.3f} tilt={tilt:+0.3f} zoom={zoom:+0.3f} auto={auto}")

                # If you want these controls to drive ROI immediately, you can do it here.
                # Example idea (you can adjust mapping):
                # - pan/tilt in [-1..1] => move ROI center +/- some pixels
                # - zoom in [-1..1] => adjust roi_hfov_deg
                # - auto => ignore manual pan/tilt
                #
                # (Left as a hook; you said you’re integrating.)

                if self.on_control:
                    try:
                        self.on_control(data, addr)
                    except Exception as e:
                        print(f"[VID-CTRL] on_control error: {e}")

            except socket.timeout:
                continue
            except Exception as e:
                if not self._stop.is_set():
                    print(f"[VID-CTRL] recv error: {e}")
                time.sleep(0.2)


# ------------------ basic manual test -------------------
if __name__ == "__main__":
    # Minimal smoke test: start control RX thread only (no ffmpeg) if desired.
    # Change side_a_ip/video_port/v4l2_device to suit your setup.
    vs = VideoStreamer(
        video_port=7001,
        ctrl_port=6001,
        width=1280,
        height=720,
        fps=8,
        bitrate_bps=2_000_000,
        v4l2_device="/dev/video0",
        input_format="mjpeg",
        side_a_ip="192.168.144.11",
        fisheye_crop_enable=False,  # set True if you have v360 available
    )
    vs.start()

    try:
        while True:
            time.sleep(1.0)
            vs.ensure_running()
    except KeyboardInterrupt:
        pass
    finally:
        vs.stop()
        vs.close()
