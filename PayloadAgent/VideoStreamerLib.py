# ---------- Video / FFmpeg wrapper ----------
from typing import Optional, Dict, Callable
import time
import threading
import socket
import shutil
import shlex
import subprocess

class VideoStreamer:
    """
    Owns:
      - ffmpeg command build/launch/stop
      - local listener on UDP:VIDEO_CTRL_PORT for incoming control commands (raw bytes for now)
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
            return subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False).returncode
        except Exception:
            return 1

    def _try_set_camera_parm(self) -> None:
        if self._have_cmd("v4l2-ctl"):
            self._run_cmd(["v4l2-ctl", "-d", self.v4l2_device, "--set-parm", str(self.fps)])

    def _build_ffmpeg_cmd(self, encoder: str) -> list[str]:
        # Common TS-over-UDP packet sizing. Also set a UDP socket buffer to reduce drops.
        url = f"udp://{self.side_a_ip}:{self.video_port}?pkt_size=1316&buffer_size=1048576"

        v4l2_input = [
            "-f", "v4l2",
            "-input_format", self.input_format,
            "-video_size", f"{self.width}x{self.height}",
            "-framerate", str(self.fps),
            "-i", self.v4l2_device,
        ]

        if encoder == "h264_v4l2m2m":
            enc = [
                "-c:v", "h264_v4l2m2m",
                "-pix_fmt", "nv12",
                "-b:v", str(self.bitrate_bps),
                "-maxrate", str(self.bitrate_bps),
                "-bufsize", str(self.bitrate_bps // 2),
                "-g", str(max(2, self.fps) * 2),
                "-bf", "0",
            ]
        else:
            enc = [
                "-c:v", "libx264",
                "-preset", "ultrafast",
                "-tune", "zerolatency",
                "-threads", "2",
                "-pix_fmt", "yuv420p",
                "-b:v", str(self.bitrate_bps),
                "-maxrate", str(self.bitrate_bps),
                "-bufsize", str(self.bitrate_bps // 2),
                "-g", str(max(2, self.fps) * 2),
            ]

        base = [
            "ffmpeg", "-hide_banner", "-loglevel", "warning",
            "-rtbufsize", "32M",
            "-thread_queue_size", "256",
            *v4l2_input,
            *enc,
            "-vsync", "cfr",
            "-r", str(self.fps),
            "-an",
            "-f", "mpegts",
            "-muxdelay", "0",
            "-muxpreload", "0",
            url,
        ]

        prio = (["nice", "-n", "10", "ionice", "-c2", "-n", "7"]
                if self._have_cmd("nice") and self._have_cmd("ionice") else [])
        return prio + base

    # ---- lifecycle ----
    def start(self) -> None:
        if self._rx_thread and self._rx_thread.is_alive():
            return
        self._stop.clear()

        self._rx_thread = threading.Thread(target=self._rx_loop, name="VideoCtrlRx", daemon=True)
        self._rx_thread.start()

        # Launch ffmpeg initially
        self.ensure_running()

    def stop(self) -> None:
        self._stop.set()
        self.stop_ffmpeg()
        if self._rx_thread:
            self._rx_thread.join(timeout=1.0)

    def ensure_running(self) -> None:
        if self._ff is None or self._ff.poll() is not None:
            if self._ff is not None:
                self._last_exit_code = self._ff.returncode
                print(f"[SUP] FFmpeg died (code {self._ff.returncode}); will recover...")
            self._restart_count += 1
            self._launch_ffmpeg()

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

    # ---- internals ----
    def _launch_ffmpeg(self) -> None:
        self._try_set_camera_parm()

        cmd_hw = self._build_ffmpeg_cmd("h264_v4l2m2m")
        print("[FFMPEG] (HW) ", " ".join(shlex.quote(x) for x in cmd_hw))
        ff = subprocess.Popen(cmd_hw)
        time.sleep(1.0)

        if ff.poll() is not None and ff.returncode != 0:
            print(f"[FFMPEG] Hardware encoder failed (code {ff.returncode}). Trying software libx264...")
            cmd_sw = self._build_ffmpeg_cmd("libx264")
            print("[FFMPEG] (SW) ", " ".join(shlex.quote(x) for x in cmd_sw))
            ff = subprocess.Popen(cmd_sw)
            self._encoder_in_use = "libx264"
        else:
            self._encoder_in_use = "h264_v4l2m2m"

        self._ff = ff

    def _rx_loop(self) -> None:
        self._ctrl_sock.settimeout(0.5)
        print(f"[VID-CTRL] Listening for control on UDP 0.0.0.0:{self.ctrl_port} (RX-only; raw bytes)")
        while not self._stop.is_set():
            try:
                data, addr = self._ctrl_sock.recvfrom(4096)
                if not data:
                    continue
                self._last_cmd = data
                self._last_cmd_from = addr
                preview = data[:120].decode("utf-8", errors="replace").replace("\n", "\\n")
                print(f"[VID-CTRL] <- {addr[0]}:{addr[1]} bytes={len(data)} head='{preview}'")

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

