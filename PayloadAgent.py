#!/usr/bin/env python3
"""
PayloadAgent.py  (UDP version)

Now:
- UDP:6000 is now the "Video Control" channel:
- VIDEO stream (MPEG-TS over UDP) still goes to UDP:7000
- CoT goes to UDP:8000
- STATUS is sent to UDP:9000

Side-A IP (receiver): 192.168.144.11
Gateway IP (reachability ping): 192.168.144.10
"""

import os
import sys
import time
import math
import signal
import socket
import shutil
import threading
import subprocess
import shlex
from typing import Optional, Dict, Callable
from datetime import datetime, timedelta, timezone

# ---------- Network & device config ----------
SIDE_A_IP        = "192.168.144.11"      # Side-A receiver
GATEWAY_IP       = "192.168.144.10"      # Herelink Air Unit (reachability ping)
IFACE            = "eth0"                # interface to bounce on faults

VIDEO_CTRL_PORT  = 6000                  # UDP video control ONLY (rx only)
VIDEO_PORT       = 7000                  # UDP video (mpegts)
COT_PORT         = 8000                  # UDP CoT
HEARTBEAT_PORT   = 9000                  # UDP heartbeat

# ---------- Video settings (optimised for Pi but using MJPEG input) ----------
FRAME_WIDTH      = 640
FRAME_HEIGHT     = 480
FRAME_RATE       = 8
BITRATE_BPS      = 2_000_000
V4L2_DEVICE      = "/dev/video0"
INPUT_FORMAT     = "mjpeg"

# ---------- MAVLink settings ----------
DEFAULT_DEVICE   = "/dev/serial0"
DEFAULT_BAUD     = 115200

DEFAULT_REQ_INTERVALS_HZ: Dict[str, int] = {
    "ATTITUDE": 20,
    "GLOBAL_POSITION_INT": 5,
    "GPS_RAW_INT": 5,
    "VFR_HUD": 5,
    "SYS_STATUS": 1,
    "BATTERY_STATUS": 1,
    "HIGHRES_IMU": 10,
    "SCALED_PRESSURE": 5,
    "LOCAL_POSITION_NED": 5,
}

# ---------- Utilities ----------
def run_cmd(cmd: list[str]) -> int:
    try:
        return subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False).returncode
    except Exception:
        return 1

def have_cmd(name: str) -> bool:
    return shutil.which(name) is not None

def ping_once(ip: str, timeout_s: float = 1.0) -> bool:
    return run_cmd(["ping", "-c", "1", "-W", str(int(timeout_s)), ip]) == 0

def bounce_interface(iface: str) -> None:
    print(f"[RECOVERY] Bouncing interface {iface}...")

    def ip_down_up(use_sudo: bool) -> bool:
        base = ["ip", "link", "set", iface]
        down = (["sudo", "-n"] + base + ["down"]) if use_sudo else (base + ["down"])
        up   = (["sudo", "-n"] + base + ["up"])   if use_sudo else (base + ["up"])
        rc1 = run_cmd(down); time.sleep(3.0)
        rc2 = run_cmd(up)
        flush = (["sudo", "-n", "ip", "neigh", "flush", "dev", iface]) if use_sudo else (["ip", "neigh", "flush", "dev", iface])
        run_cmd(flush)
        return rc1 == 0 and rc2 == 0

    if os.geteuid() == 0 and have_cmd("ip"):
        if ip_down_up(False):
            print("[RECOVERY] Link bounced via ip (root)."); return
    if have_cmd("sudo") and have_cmd("ip"):
        if ip_down_up(True):
            print("[RECOVERY] Link bounced via sudo ip."); return
    if have_cmd("nmcli"):
        rc_disc = run_cmd(["nmcli", "device", "disconnect", iface]); time.sleep(3.0)
        rc_conn = run_cmd(["nmcli", "device", "connect", iface])
        if rc_disc == 0 and rc_conn == 0:
            if have_cmd("ip"):
                run_cmd(["ip", "neigh", "flush", "dev", iface])
            print("[RECOVERY] Link bounced via nmcli."); return
    if have_cmd("ifconfig"):
        rc1 = run_cmd(["ifconfig", iface, "down"]); time.sleep(3.0)
        rc2 = run_cmd(["ifconfig", iface, "up"])
        if rc1 == 0 and rc2 == 0:
            if have_cmd("ip"):
                run_cmd(["ip", "neigh", "flush", "dev", iface])
            print("[RECOVERY] Link bounced via ifconfig."); return
    print("[RECOVERY] Failed to bounce interface automatically. Check permissions/sudoers.]")

def wait_for_gateway(ip: str, timeout_s: float = 40.0, interval_s: float = 1.0) -> bool:
    print(f"[RECOVERY] Waiting for gateway {ip} to respond...")
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        if run_cmd(["ping", "-c", "1", "-W", "1", ip]) == 0:
            print("[RECOVERY] Gateway reachable."); return True
        time.sleep(interval_s)
    print("[RECOVERY] Gateway still unreachable after wait."); return False

# ---------- UDP sender + instrumentation ----------
class UdpSender:
    """
    Simple UDP sender with optional periodic stats.
    """
    def __init__(self, dest_ip: str, dest_port: int, bind_ip: str = "0.0.0.0", bind_port: int = 0) -> None:
        self.dest = (dest_ip, int(dest_port))
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((bind_ip, int(bind_port)))

        self._lock = threading.Lock()
        self._bytes = 0
        self._pkts = 0
        self._t0 = time.time()

    def close(self) -> None:
        try:
            self.sock.close()
        except Exception:
            pass

    def send(self, data: bytes, label: str = "") -> None:
        if not data:
            return
        try:
            n = self.sock.sendto(data, self.dest)
            now = time.time()
            with self._lock:
                self._bytes += n
                self._pkts += 1
                dt = now - self._t0
                # preview = data[:80]
                # preview_txt = preview.decode("utf-8", errors="replace").replace("\n", "\\n")
                # print(f"[UDP {label}] -> {self.dest[0]}:{self.dest[1]}  bytes={n}  head='{preview_txt}'")

                if dt >= 2.0:
                    # bps = self._bytes / dt
                    # pps = self._pkts / dt
                    # print(f"[UDP {label}] STATS {self.dest[0]}:{self.dest[1]}  {bps:.0f} B/s  {pps:.1f} pkt/s  over {dt:.1f}s")
                    self._bytes = 0
                    self._pkts = 0
                    self._t0 = now
        except Exception as e:
            raise ConnectionError(f"UDP send failed to {self.dest}: {e}") from e

# ---------- Heartbeat (UDP) ----------
class PayloadStatusSource:
    """
    Periodically sends a simple heartbeat line over UDP.
    """
    def __init__(self, dest_ip: str, port: int = HEARTBEAT_PORT,
                 interval_s: float = 0.25) -> None:
        self.dest_ip = dest_ip
        self.port = int(port)
        self.interval_s = float(interval_s)

        self._udp = UdpSender(dest_ip, port)
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._fault = threading.Event()
        self._counter = 1

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._fault.clear()
        self._thread = threading.Thread(target=self._run, name="PayloadStatusSource", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        try:
            self._udp.send(b"STOPPING_HEARTBEAT\n", label="HB")
        except Exception:
            pass
        try:
            self._udp.close()
        except Exception:
            pass
        if self._thread:
            self._thread.join(timeout=1.5)

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def has_fault(self) -> bool:
        return self._fault.is_set()

    def _run(self) -> None:
        try:
            self._udp.send(b"STARTING_HEARTBEAT\n", label="HB")
            print(f"[UDP HB] Sending to {self.dest_ip}:{self.port}")
        except Exception as e:
            print(f"[UDP HB] start failed: {e}")
            self._fault.set()
            return

        while not self._stop.is_set():
            try:
                msg = f"Test {self._counter}\n".encode("utf-8")
                self._udp.send(msg, label="HB")
                self._counter += 1
            except Exception as e:
                print(f"[UDP HB] send error: {e}")
                self._fault.set()
                break

            if self._stop.wait(self.interval_s):
                break

# ---------- Video / FFmpeg wrapper ----------
class VideoStreamer:
    """
    Owns:
      - ffmpeg command build/launch/stop
      - local listener on UDP:VIDEO_CTRL_PORT for incoming control commands (raw bytes for now)

    NOTE:
      - VIDEO_CTRL_PORT is RX-only (no status transmission).
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

# ---------- Cursor-on-Target bridge (UDP) ----------
from xml.etree import ElementTree as ET  # noqa: E402

class CursorOnTargetBridge:
    """
    Convert MAVLink GLOBAL_POSITION_INT to Cursor-on-Target (CoT) and send over UDP.
    """
    def __init__(self, uid: str, cot_type: str, udp_sender: UdpSender,
                 min_interval: float = 0.5, callsign: Optional[str] = None,
                 geoid_height_fn=None) -> None:
        self.uid = uid
        self.cot_type = cot_type
        self.udp = udp_sender
        self.min_interval = float(min_interval)
        self.callsign = callsign
        self.geoid_height_fn = geoid_height_fn or (lambda lat, lon: 0.0)
        self._last_sent_time = 0.0

    def process_global_position_int(self, msg) -> None:
        now = time.time()
        if (now - self._last_sent_time) < self.min_interval:
            return
        try:
            lat = getattr(msg, "lat", 0) / 1e7
            lon = getattr(msg, "lon", 0) / 1e7
            alt_msl_m = getattr(msg, "alt", 0) / 1000.0
            v_north_ms = getattr(msg, "vx", 0) / 100.0
            v_east_ms  = getattr(msg, "vy", 0) / 100.0
            v_down_ms  = getattr(msg, "vz", 0) / 100.0

            groundspeed_ms = math.hypot(v_north_ms, v_east_ms)
            speed_3d_ms = math.sqrt(v_north_ms**2 + v_east_ms**2 + v_down_ms**2)
            course_degrees = (math.degrees(math.atan2(v_east_ms, v_north_ms)) % 360.0)
            slope_degrees = math.degrees(math.atan2(v_down_ms, max(groundspeed_ms, 1e-6)))
            slope_degrees = max(min(slope_degrees, 90.0), -90.0)

            hae = alt_msl_m + float(self.geoid_height_fn(lat, lon))

            time_str = self._iso8601_now_z()
            stale_str = self._iso8601_future_z(15)

            event = ET.Element('event', {
                'version': '2.0', 'uid': self.uid, 'type': self.cot_type, 'how': 'm-g',
                'time': time_str, 'start': time_str, 'stale': stale_str
            })
            ET.SubElement(event, 'point', {
                'lat': f'{lat:.7f}', 'lon': f'{lon:.7f}', 'hae': f'{hae:.1f}',
                'ce': '5.0', 'le': '10.0'
            })
            detail = ET.SubElement(event, 'detail')
            ET.SubElement(detail, 'track', {
                'course': f'{course_degrees:.1f}', 'speed': f'{speed_3d_ms:.1f}', 'slope': f'{slope_degrees:.1f}'
            })
            if self.callsign:
                ET.SubElement(detail, 'contact', {'callsign': self.callsign})

            header = '<?xml version="1.0" encoding="UTF-8"?>\n'
            payload = (header + ET.tostring(event, encoding='unicode')).encode('utf-8') + b"\n"

            self.udp.send(payload, label="CoT")
            self._last_sent_time = now
        except Exception as e:
            print("CoT error:", e)
            time.sleep(0.2)

    def simulate_global_position_int(self) -> None:
        class Dummy: pass
        d = Dummy()
        d.lat = int(51.247773 * 1e7)
        d.lon = int(-2.775934 * 1e7)
        d.alt = int(20.0 * 1000)
        d.vx = 100
        d.vy = 100
        d.vz = 0
        self.process_global_position_int(d)

    @staticmethod
    def _iso8601_now_z() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    @staticmethod
    def _iso8601_future_z(seconds: int) -> str:
        t = datetime.now(timezone.utc) + timedelta(seconds=int(seconds))
        return t.isoformat(timespec="milliseconds").replace("+00:00", "Z")

# ---------- MAVLink interface ----------
from pymavlink import mavutil  # noqa: E402

class MavlinkInterface:
    def __init__(self, device: str = DEFAULT_DEVICE, baud: int = DEFAULT_BAUD,
                 req_intervals_hz: Optional[Dict[str, int]] = None,
                 source_system: int = 255, autoreconnect: bool = True,
                 cot_bridge: Optional[CursorOnTargetBridge] = None) -> None:
        self.device = device
        self.baud = baud
        self.req_intervals_hz = dict(req_intervals_hz or DEFAULT_REQ_INTERVALS_HZ)
        self.source_system = source_system
        self.autoreconnect = autoreconnect
        self.master: Optional[mavutil.mavfile] = None
        self._running = False
        self.cot_bridge = cot_bridge

    def open(self) -> None:
        print(f"Opening {self.device} at {self.baud} baud (RX on GPIO15)")
        self.master = mavutil.mavlink_connection(
            self.device, baud=self.baud,
            autoreconnect=self.autoreconnect,
            source_system=self.source_system
        )

    def wait_heartbeat(self, timeout: float = 10.0):
        print("Waiting for HEARTBEAT...")
        hb = self.master.wait_heartbeat(timeout=timeout)
        if hb:
            print(f"Heartbeat from sys={hb.get_srcSystem()} comp={hb.get_srcComponent()}")
        else:
            print("No heartbeat detected. Continuing to listen.")
        return hb

    @staticmethod
    def resolve_msg_id(name: str) -> Optional[int]:
        try:
            return mavutil.mavlink.map_name_to_id(name)
        except Exception:
            pass
        const_name = "MAVLINK_MSG_ID_" + name
        return getattr(mavutil.mavlink, const_name, None)

    def set_message_intervals(self) -> None:
        m = self.master
        for name, hz in self.req_intervals_hz.items():
            msgid = self.resolve_msg_id(name)
            if msgid is None:
                print("Skip unknown message name:", name)
                continue
            interval_us = int(1_000_000 / hz) if hz > 0 else -1
            m.mav.command_long_send(
                m.target_system or 1, m.target_component or 1,
                mavutil.mavlink.MAV_CMD_SET_MESSAGE_INTERVAL, 0,
                msgid, interval_us, 0, 0, 0, 0, 0
            )
            time.sleep(0.02)
        print("Requested per-message intervals.")

    def request_legacy_streams(self, rate_hz: int = 5) -> None:
        m = self.master
        streams = [
            mavutil.mavlink.MAV_DATA_STREAM_ALL,
            mavutil.mavlink.MAV_DATA_STREAM_EXTENDED_STATUS,
            mavutil.mavlink.MAV_DATA_STREAM_POSITION,
            mavutil.mavlink.MAV_DATA_STREAM_EXTRA1,
            mavutil.mavlink.MAV_DATA_STREAM_EXTRA2,
            mavutil.mavlink.MAV_DATA_STREAM_EXTRA3,
            mavutil.mavlink.MAV_DATA_STREAM_RAW_SENSORS,
            mavutil.mavlink.MAV_DATA_STREAM_RAW_CONTROLLER,
        ]
        for s in streams:
            m.mav.request_data_stream_send(m.target_system or 1, m.target_component or 1, s, rate_hz, 1)
            time.sleep(0.02)
        print("Requested legacy data streams.")

    def listen(self) -> None:
        self._running = True
        self.set_message_intervals()
        self.request_legacy_streams(rate_hz=5)
        count = 0
        try:
            while self._running:
                msg = self.master.recv_match(blocking=False)
                if msg is not None:
                    count += 1
                    self._handle_message(msg, count)
                time.sleep(0.002)
        except KeyboardInterrupt:
            print("Stopped.")
        finally:
            self._running = False

    def stop(self) -> None:
        self._running = False

    def _handle_message(self, msg: mavutil.mavlink.MAVLink_message, count: int) -> None:
        mtype = msg.get_type()

        if mtype == "HEARTBEAT":
            if self.cot_bridge:
                self.cot_bridge.simulate_global_position_int()
        elif mtype == "GLOBAL_POSITION_INT":
            lat = getattr(msg, "lat", 0) / 1e7
            lon = getattr(msg, "lon", 0) / 1e7
            alt = getattr(msg, "alt", 0) / 1000.0
            vz  = getattr(msg, "vz", 0) / 100.0
            print(f"[{count:06d}] GLOBAL_POSITION_INT lat={lat:.7f} lon={lon:.7f} alt={alt:.2f}m vz={vz:.2f}m/s")
            if self.cot_bridge:
                self.cot_bridge.process_global_position_int(msg)
        elif mtype == "GPS_RAW_INT":
            lat = getattr(msg, "lat", 0) / 1e7
            lon = getattr(msg, "lon", 0) / 1e7
            fix = getattr(msg, "fix_type", 0)
            sats = getattr(msg, "satellites_visible", 0)
            print(f"[{count:06d}] GPS_RAW_INT fix={fix} lat={lat:.7f} lon={lon:.7f} sat={sats}")
            if self.cot_bridge:
                self.cot_bridge.process_global_position_int(msg)
        else:
            pass

# ---------- Supervisor: HB + Video + MAV/CoT ----------
def supervisor_main(dev: str = DEFAULT_DEVICE, baud: int = DEFAULT_BAUD):
    print(
        f"Sending UDP heartbeat to {SIDE_A_IP}:{HEARTBEAT_PORT}, "
        f"UDP video to {SIDE_A_IP}:{VIDEO_PORT}, "
        f"UDP CoT to {SIDE_A_IP}:{COT_PORT}, "
        f"and listening for video control on local UDP:{VIDEO_CTRL_PORT} (RX-only)."
    )

    if not os.path.exists(V4L2_DEVICE):
        print(f"[VIDEO] Device {V4L2_DEVICE} not found. Is the webcam connected?")
        return

    stop_event = threading.Event()
    hb: Optional[PayloadStatusSource] = None

    # === CoT UDP sender + bridge ===
    cot_udp = UdpSender(SIDE_A_IP, COT_PORT)
    cot_bridge = CursorOnTargetBridge(
        uid="AAV-Payload-1",
        cot_type="a-f-A-M-F-Q",
        udp_sender=cot_udp,
        min_interval=0.5,
        callsign="Manta-1",
        geoid_height_fn=None,
    )

    # === MAVLink setup ===
    mav_iface = MavlinkInterface(device=dev, baud=baud, cot_bridge=cot_bridge)
    try:
        mav_iface.open()
    except Exception as e:
        print(f"[MAV] Could not open {dev}: {e}")
        print("Tip: add your user to 'dialout' group or run with sudo.")
    else:
        def mav_worker():
            try:
                mav_iface.wait_heartbeat(timeout=10)
                mav_iface.listen()
            except Exception as e:
                print("[MAV] worker error:", e)
        threading.Thread(target=mav_worker, daemon=True).start()

    # === Video streamer (ffmpeg + control rx on :6000 only) ===
    def on_video_control(data: bytes, addr: tuple[str, int]) -> None:
        txt = data.decode("utf-8", errors="replace").strip().lower()
        if txt in ("restart", "reboot_ffmpeg", "ffmpeg_restart"):
            print("[VID-CTRL] Interpreting command as restart request (temporary behaviour).")
            video.stop_ffmpeg()
        elif txt in ("stop", "ffmpeg_stop"):
            print("[VID-CTRL] Interpreting command as stop request (temporary behaviour).")
            video.stop_ffmpeg()
        # formats later; ignore everything else for now

    video = VideoStreamer(
        side_a_ip=SIDE_A_IP,
        video_port=VIDEO_PORT,
        ctrl_port=VIDEO_CTRL_PORT,
        width=FRAME_WIDTH,
        height=FRAME_HEIGHT,
        fps=FRAME_RATE,
        bitrate_bps=BITRATE_BPS,
        v4l2_device=V4L2_DEVICE,
        input_format=INPUT_FORMAT,
        on_control=on_video_control,
    )
    video.start()

    def clean_exit(*_):
        stop_event.set()
        print("Stopping...")
        try:
            video.stop()
        except Exception:
            pass
        try:
            if hb:
                hb.stop()
        except Exception:
            pass
        try:
            cot_udp.close()
        except Exception:
            pass
        try:
            mav_iface.stop()
        except Exception:
            pass
        try:
            video.close()
        except Exception:
            pass
        sys.exit(0)

    signal.signal(signal.SIGINT, clean_exit)
    signal.signal(signal.SIGTERM, clean_exit)

    print(
        f"Starting webcam stream: {FRAME_WIDTH}x{FRAME_HEIGHT}@{FRAME_RATE} "
        f"from {V4L2_DEVICE} -> udp://{SIDE_A_IP}:{VIDEO_PORT}"
    )
    print(f"Video control: local UDP :{VIDEO_CTRL_PORT} (RX-only)")
    print(f"Heartbeat: Side-A {SIDE_A_IP}:{HEARTBEAT_PORT}")

    retry_delay = 1.0
    while not stop_event.is_set():
        if not ping_once(GATEWAY_IP):
            print("[SUP] Gateway ping failed; starting recovery...")
            bounce_interface(IFACE)
            waited = wait_for_gateway(GATEWAY_IP, timeout_s=25.0)
            if not waited:
                time.sleep(min(retry_delay, 5.0))
                retry_delay = min(retry_delay * 2, 10.0)
                continue

        # Heartbeat (re)start (9000)
        if hb is None or not hb.is_running() or hb.has_fault():
            try:
                if hb:
                    hb.stop()
            except Exception:
                pass
            hb = PayloadStatusSource(SIDE_A_IP, HEARTBEAT_PORT, interval_s=1.0)
            hb.start()

        # Ensure ffmpeg running
        video.ensure_running()

        # Basic pacing + light monitoring
        for _ in range(10):  # ~2.5s total
            if stop_event.is_set():
                clean_exit()
            if hb and hb.has_fault():
                print("[SUP] Heartbeat fault detected; initiating recovery...")
                break
            if video._ff is None or (video._ff is not None and video._ff.poll() is not None):
                print("[SUP] FFmpeg not running; restarting...")
                break
            time.sleep(0.25)

        # If HB fault, bounce and try again
        if hb and hb.has_fault():
            try:
                hb.stop()
            except Exception:
                pass
            hb = None
            try:
                video.stop_ffmpeg()
            except Exception:
                pass
            bounce_interface(IFACE)
            wait_for_gateway(GATEWAY_IP, timeout_s=25.0)
            time.sleep(1.0)
            retry_delay = 1.0
            continue

    clean_exit()

# ---------- CLI ----------
if __name__ == "__main__":
    dev = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DEVICE
    baud = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_BAUD
    supervisor_main(dev, baud)
