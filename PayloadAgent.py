#!/usr/bin/env python3
"""
BridgeAgent.py

Merged agent with:
- Heartbeat over TCP -> SERVER_IP:6000 (via PayloadStatusSource using TcpClient)
- H.264/MPEG-TS video -> SERVER_IP:6001 (ffmpeg supervised)
- Cursor-on-Target (from MAVLink GLOBAL_POSITION_INT) -> SERVER_IP:6002 (TcpClient)

Requires:
  - ffmpeg in PATH
  - Python: pymavlink
  - (optional) v4l2-ctl in PATH for trying to set camera FPS

Run:
  python3 BridgeAgent.py [/dev/serialX] [baud]
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
import select
from typing import Optional, Dict
from datetime import datetime, timedelta, timezone

# ---------- Network & device config ----------
SERVER_IP        = "192.168.144.11"      # Remote server that receives HB/video/CoT
GATEWAY_IP       = "192.168.144.10"      # Herelink Air Unit (reachability ping)
IFACE            = "eth0"                # interface to bounce on faults

HEARTBEAT_PORT   = 6000                  # heartbeat
VIDEO_TCP_PORT   = 6001                  # video
COT_TCP_PORT     = 6002                  # CoT receiver port (CursorOnTargetBridge output goes here)

# ---------- Video settings (optimised for Pi but using MJPEG input) ----------
FRAME_WIDTH      = 1280
FRAME_HEIGHT     =  720
FRAME_RATE       = 8             # lower FPS reduces load & bandwidth
BITRATE_BPS      = 2_000_000     # target encoder bitrate
V4L2_DEVICE      = "/dev/video0"
INPUT_FORMAT     = "mjpeg"       # KEEP MJPEG as requested

# ---------- MAVLink settings ----------
DEFAULT_DEVICE   = "/dev/serial0"        # GPIO15 (RXD0) on Raspberry Pi
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

# ---------- Simple TCP client wrapper ----------
class TcpClient:
    def __init__(self, host: str, port: int, connect_timeout: float = 5.0,
                 keepalive: bool = True, keepalive_idle: Optional[int] = None,
                 keepalive_intvl: Optional[int] = None, keepalive_cnt: Optional[int] = None) -> None:
        self.host = host; self.port = int(port)
        self.connect_timeout = connect_timeout
        self.keepalive = keepalive
        self.keepalive_idle = keepalive_idle
        self.keepalive_intvl = keepalive_intvl
        self.keepalive_cnt = keepalive_cnt
        self._sock: Optional[socket.socket] = None

    def connect(self, retry: int = 0, delay: float = 1.0) -> None:
        last_exc: Optional[BaseException] = None
        for attempt in range(retry + 1):
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(self.connect_timeout); s.connect((self.host, self.port))
                self._configure_keepalive(s); self._sock = s
                print(f"TCP connected to {self.host}:{self.port}"); return
            except BaseException as e:
                last_exc = e
                try: s.close()
                except Exception: pass
                if attempt < retry: time.sleep(max(0.0, delay))
        raise ConnectionError(f"Failed to connect to {self.host}:{self.port}") from last_exc

    def close(self) -> None:
        if self._sock:
            try: self._sock.shutdown(socket.SHUT_RDWR)
            except Exception: pass
            try: self._sock.close()
            finally: self._sock = None
            print("TCP connection closed")

    def is_connected(self) -> bool: return self._sock is not None

    def send_bytes(self, data: bytes) -> None:
        if not self._sock: raise RuntimeError("Not connected")
        totalsent = 0
        while totalsent < len(data):
            sent = self._sock.send(data[totalsent:])
            if sent == 0:
                self.close(); raise ConnectionError("Socket connection broken")
            totalsent += sent

    def send_line(self, text: str, encoding: str = "utf-8") -> None:
        self.send_bytes((text + "\n").encode(encoding))

    def recv_some(self, max_bytes: int = 1024, timeout: float = 0.0) -> bytes:
        if not self._sock:
            return b""
        rlist, _, _ = select.select([self._sock], [], [], max(0.0, timeout))
        if rlist:
            try:
                data = self._sock.recv(max_bytes)
                if data == b"":  # remote closed
                    self.close()
                return data
            except Exception:
                self.close()
                return b""
        return b""

    def _configure_keepalive(self, s: socket.socket) -> None:
        if not self.keepalive: return
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            if hasattr(socket, "TCP_KEEPIDLE") and self.keepalive_idle is not None:
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, int(self.keepalive_idle))
            if hasattr(socket, "TCP_KEEPINTVL") and self.keepalive_intvl is not None:
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, int(self.keepalive_intvl))
            if hasattr(socket, "TCP_KEEPCNT") and self.keepalive_cnt is not None:
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, int(self.keepalive_cnt))
        except Exception:
            pass

# ---------- Heartbeat ----------
class PayloadStatusSource:
    """
    Periodically sends a simple heartbeat line to a TCP server using TcpClient,
    and reads any replies. Signals a 'fault' if the connection breaks.
    """
    def __init__(self, server_ip: str, port: int = HEARTBEAT_PORT,
                 interval_s: float = 0.25, connect_timeout: float = 5.0) -> None:
        self.server_ip = server_ip
        self.port = int(port)
        self.interval_s = float(interval_s)
        self.connect_timeout = connect_timeout

        self._client = TcpClient(server_ip, port, connect_timeout=self.connect_timeout)
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
            if self._client.is_connected():
                try: self._client.send_line("STOPPING_HEARTBEAT")
                except Exception: pass
                self._client.close()
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
            self._client.connect(retry=0)
            try: self._client.send_line("STARTING_HEARTBEAT")
            except Exception: pass
            print("[TCP HB] Connected (PayloadStatusSource).")
        except Exception as e:
            print(f"[TCP HB] connect failed: {e}")
            self._fault.set()
            return

        while not self._stop.is_set():
            try:
                self._client.send_line(f"Hello world {self._counter}")
                resp = self._client.recv_some(max_bytes=1024, timeout=0.2)
                if resp == b"" and not self._client.is_connected():
                    print("[TCP HB] server closed connection")
                    self._fault.set()
                    break
                if resp:
                    print(f"[TCP HB]  <-- {resp.decode(errors='replace').strip()}")
                self._counter += 1
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, ConnectionError) as e:
                print(f"[TCP HB] link error: {e}")
                self._fault.set()
                break
            except Exception as e:
                print(f"[TCP HB] unexpected: {e}")
                self._fault.set()
                break

            if self._stop.wait(self.interval_s):
                break

# ---------- Cursor-on-Target bridge ----------
from xml.etree import ElementTree as ET  # noqa: E402
class CursorOnTargetBridge:
    """
    Convert MAVLink GLOBAL_POSITION_INT to Cursor-on-Target (CoT) and send over TCP.
    """
    def __init__(self, uid: str, cot_type: str, tcp_client: TcpClient,
                 min_interval: float = 0.5, callsign: Optional[str] = None,
                 geoid_height_fn=None) -> None:
        self.uid = uid; self.cot_type = cot_type; self.tcp_client = tcp_client
        self.min_interval = float(min_interval); self.callsign = callsign
        self.geoid_height_fn = geoid_height_fn or (lambda lat, lon: 0.0)
        self._last_sent_time = 0.0

    def process_global_position_int(self, msg) -> None:
        now = time.time()
        if (now - self._last_sent_time) < self.min_interval: return
        try:
            lat = getattr(msg, "lat", 0) / 1e7
            lon = getattr(msg, "lon", 0) / 1e7
            # Fixed for your test
            #lat = 51.247773
            #lon = -2.775934
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

            if self.tcp_client.is_connected():
                print("Sending:" + str(payload))
                self.tcp_client.send_bytes(payload)
                self._last_sent_time = now
                print("[CoT] sent")
            else:
                try:
                    self.tcp_client.connect(retry=0)
                except Exception:
                    pass
        except Exception as e:
            print("CoT error:", e)
            time.sleep(1.0)

    def simulate_global_position_int(self) -> None:
        now = time.time()
        lat = 51.247773
        lon = -2.775934
        alt_msl_m  = 20.0 / 1000.0
        v_north_ms = 1.0 / 100.0
        v_east_ms  = 1.0 / 100.0
        v_down_ms  = 0.0 / 100.0

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

        if self.tcp_client.is_connected():
            print("Sending:" + str(payload))
            self.tcp_client.send_bytes(payload)
            self._last_sent_time = now
            print("[CoT] sent")
        else:
            try:
                self.tcp_client.connect(retry=0)
            except Exception:
                pass

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
        self.device = device; self.baud = baud
        self.req_intervals_hz = dict(req_intervals_hz or DEFAULT_REQ_INTERVALS_HZ)
        self.source_system = source_system; self.autoreconnect = autoreconnect
        self.master: Optional[mavutil.mavfile] = None
        self._running = False; self.cot_bridge = cot_bridge

    def open(self) -> None:
        print(f"Opening {self.device} at {self.baud} baud (RX on GPIO15)")
        self.master = mavutil.mavlink_connection(self.device, baud=self.baud,
                                                 autoreconnect=self.autoreconnect, source_system=self.source_system)

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
        try: return mavutil.mavlink.map_name_to_id(name)
        except Exception: pass
        const_name = "MAVLINK_MSG_ID_" + name
        return getattr(mavutil.mavlink, const_name, None)

    def set_message_intervals(self) -> None:
        m = self.master
        for name, hz in self.req_intervals_hz.items():
            msgid = self.resolve_msg_id(name)
            if msgid is None:
                print("Skip unknown message name:", name); continue
            interval_us = int(1_000_000 / hz) if hz > 0 else -1
            m.mav.command_long_send(m.target_system or 1, m.target_component or 1,
                                    mavutil.mavlink.MAV_CMD_SET_MESSAGE_INTERVAL, 0,
                                    msgid, interval_us, 0,0,0,0,0)
            time.sleep(0.02)
        print("Requested per-message intervals.")

    def request_legacy_streams(self, rate_hz: int = 5) -> None:
        m = self.master
        streams = [
            mavutil.mavlink.MAV_DATA_STREAM_ALL, mavutil.mavlink.MAV_DATA_STREAM_EXTENDED_STATUS,
            mavutil.mavlink.MAV_DATA_STREAM_POSITION, mavutil.mavlink.MAV_DATA_STREAM_EXTRA1,
            mavutil.mavlink.MAV_DATA_STREAM_EXTRA2, mavutil.mavlink.MAV_DATA_STREAM_EXTRA3,
            mavutil.mavlink.MAV_DATA_STREAM_RAW_SENSORS, mavutil.mavlink.MAV_DATA_STREAM_RAW_CONTROLLER,
        ]
        for s in streams:
            m.mav.request_data_stream_send(m.target_system or 1, m.target_component or 1, s, rate_hz, 1)
            time.sleep(0.02)
        print("Requested legacy data streams.")

    def listen(self) -> None:
        self._running = True
        self.set_message_intervals()
        self.request_legacy_streams(rate_hz=5)
        last_status = time.time(); count = 0
        try:
            while self._running:
                msg = self.master.recv_match(blocking=False)
                if msg is not None:
                    count += 1; self._handle_message(msg, count)
                if time.time() - last_status >= 5.0:
                    last_status = time.time()
                    ver = "v2" if self.master.mavlink20() else "v1"
                    print(f"...listening ({ver}), received {count} messages")
                time.sleep(0.002)
        except KeyboardInterrupt:
            print("Stopped.")
        finally:
            self._running = False

    def stop(self) -> None: self._running = False

    def _handle_message(self, msg: mavutil.mavlink.MAVLink_message, count: int) -> None:
        mtype = msg.get_type()

        if mtype == "HEARTBEAT":
            print(f"[{count:06d}] HEARTBEAT sys={msg.get_srcSystem()} comp={msg.get_srcComponent()} "
                  f"type={getattr(msg,'type',None)} autopilot={getattr(msg,'autopilot',None)} "
                  f"base_mode={getattr(msg,'base_mode',None)}")
            self.cot_bridge.simulate_global_position_int()
        elif mtype == "GLOBAL_POSITION_INT":
            lat = getattr(msg,"lat",0)/1e7; lon = getattr(msg,"lon",0)/1e7
            alt = getattr(msg,"alt",0)/1000.0; vz = getattr(msg,"vz",0)/100.0
            print(f"[{count:06d}] GLOBAL_POSITION_INT lat={lat:.7f} lon={lon:.7f} alt={alt:.2f}m vz={vz:.2f}m/s")
            if self.cot_bridge:
                self.cot_bridge.process_global_position_int(msg)
        elif mtype == "ATTITUDE":
            #print(f"[{count:06d}] ATTITUDE roll={getattr(msg,'roll',0.0):.3f} "
            #      f"pitch={getattr(msg,'pitch',0.0):.3f} yaw={getattr(msg,'yaw',0.0):.3f}")
            pass
        elif mtype == "GPS_RAW_INT":
            lat = getattr(msg,"lat",0)/1e7; lon = getattr(msg,"lon",0)/1e7
            fix = getattr(msg,"fix_type",0); sats = getattr(msg,"satellites_visible",0)
            print(f"[{count:06d}] GPS_RAW_INT fix={fix} lat={lat:.7f} lon={lon:.7f} sat={sats}")
            if self.cot_bridge:
                self.cot_bridge.process_global_position_int(msg)
        elif mtype == "SYS_STATUS":
            vbat = getattr(msg,"voltage_battery",0)/1000.0; cur = getattr(msg,"current_battery",0)/100.0
            rem = getattr(msg,"battery_remaining",-1)
            #print(f"[{count:06d}] SYS_STATUS vbat={vbat:.2f}V cur={cur:.2f}A rem={rem}pct")
        else:
            pass # print(f"[{count:06d}] {mtype}")

# ---------- FFmpeg launcher (low priority, MJPEG input kept) ----------
def try_set_camera_parm(device: str, fps: int) -> None:
    """
    Best-effort: ask the UVC cam to run at our FPS. Many MJPEG cams ignore this.
    """
    if have_cmd("v4l2-ctl"):
        run_cmd(["v4l2-ctl", "-d", device, "--set-parm", str(fps)])

def build_ffmpeg_cmd(server_ip: str, tcp_port: int, fps: int, w: int, h: int,
                     device: str, input_format: str, encoder: str):
    url = f"tcp://{server_ip}:{tcp_port}"

    # Input: keep MJPEG (requested). We still ask for lower framerate; some cams obey.
    v4l2_input = [
        "-f","v4l2",
        "-input_format", input_format,       # "mjpeg"
        "-video_size", f"{w}x{h}",
        "-framerate", str(fps),
        "-i", device,
    ]

    if encoder == "h264_v4l2m2m":
        enc = [
            "-c:v","h264_v4l2m2m",
            "-pix_fmt","nv12",               # friendly for HW encoder
            "-b:v", str(BITRATE_BPS),
            "-maxrate", str(BITRATE_BPS),
            "-bufsize", str(BITRATE_BPS//2),
            "-g", str(max(2, fps)*2),
            "-bf","0",
        ]
    else:
        enc = [
            "-c:v","libx264",
            "-preset","ultrafast",
            "-tune","zerolatency",
            "-threads","2",
            "-pix_fmt","yuv420p",
            "-b:v", str(BITRATE_BPS),
            "-maxrate", str(BITRATE_BPS),
            "-bufsize", str(BITRATE_BPS//2),
            "-g", str(max(2, fps)*2),
        ]

    # We keep constant pacing and low mux latency.
    # NOTE: With MJPEG input, the camera may still deliver 30 fps; we clamp output to fps with -vsync cfr -r.
    return [
        "ffmpeg","-hide_banner","-loglevel","warning",
        "-rtbufsize","32M",
        "-thread_queue_size","256",
        *v4l2_input,
        *enc,
        "-vsync","cfr",
        "-r", str(fps),
        "-an",
        "-f","mpegts",
        "-muxdelay","0",
        "-muxpreload","0",
        url
    ]

def launch_ffmpeg(server_ip: str, tcp_port: int, fps: int, w: int, h: int,
                  device: str, input_format: str) -> subprocess.Popen:
    # Try to set camera FPS (best effort; many MJPEG cams ignore)
    try_set_camera_parm(device, fps)

    # Prefer hardware encoder; run at lower CPU/IO priority to protect MAVLink.
    base_hw = build_ffmpeg_cmd(server_ip, tcp_port, fps, w, h, device, input_format, "h264_v4l2m2m")
    prio = (["nice","-n","10","ionice","-c2","-n","7"] if have_cmd("nice") and have_cmd("ionice") else [])
    cmd_hw = prio + base_hw
    print("[FFMPEG] (HW) ", " ".join(shlex.quote(x) for x in cmd_hw))
    ff = subprocess.Popen(cmd_hw); time.sleep(1.0)

    if ff.poll() is not None and ff.returncode != 0:
        print(f"[FFMPEG] Hardware encoder failed (code {ff.returncode}). Trying software libx264...")
        base_sw = build_ffmpeg_cmd(server_ip, tcp_port, fps, w, h, device, input_format, "libx264")
        cmd_sw = prio + base_sw
        print("[FFMPEG] (SW) ", " ".join(shlex.quote(x) for x in cmd_sw))
        ff = subprocess.Popen(cmd_sw)
    return ff

def stop_ffmpeg(ff: Optional[subprocess.Popen]):
    if not ff: return
    try:
        if ff.poll() is None:
            ff.terminate()
            try:
                ret = ff.wait(timeout=2); print(f"[FFMPEG] exited with code {ret}")
            except Exception:
                ff.kill()
    except Exception:
        pass

# ---------- Supervisor: HB + Video + MAV/CoT ----------
def supervisor_main(dev: str = DEFAULT_DEVICE, baud: int = DEFAULT_BAUD):
    print(f"Connecting heartbeat TCP to {SERVER_IP}:{HEARTBEAT_PORT}, "
          f"video TCP to {SERVER_IP}:{VIDEO_TCP_PORT}, "
          f"and CoT TCP to {SERVER_IP}:{COT_TCP_PORT} ...")

    if not os.path.exists(V4L2_DEVICE):
        print(f"[VIDEO] Device {V4L2_DEVICE} not found. Is the webcam connected?")
        return

    stop_event  = threading.Event()

    hb: Optional[PayloadStatusSource] = None
    ff: Optional[subprocess.Popen] = None

    # === MAVLink + CoT setup ===
    cot_tcp = TcpClient(SERVER_IP, COT_TCP_PORT, connect_timeout=5.0)
    try:
        cot_tcp.connect(retry=3, delay=1.0)
    except Exception as e:
        print(f"[CoT] WARNING: Could not connect to {SERVER_IP}:{COT_TCP_PORT}: {e}")

    cot_bridge = CursorOnTargetBridge(
        uid="AAV-Payload-1",
        cot_type="a-f-A-M-F-Q",
        tcp_client=cot_tcp,
        min_interval=0.5,
        callsign="Manta-1",
        geoid_height_fn=None,
    )

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

    def clean_exit(*_):
        stop_event.set()
        print("Stopping...")
        stop_ffmpeg(ff)
        try:
            if hb: hb.stop()
        except Exception: pass
        try:
            cot_tcp.close()
        except Exception: pass
        try:
            mav_iface.stop()
        except Exception: pass
        sys.exit(0)

    signal.signal(signal.SIGINT, clean_exit)
    signal.signal(signal.SIGTERM, clean_exit)

    print(f"Starting webcam stream: {FRAME_WIDTH}x{FRAME_HEIGHT}@{FRAME_RATE} from {V4L2_DEVICE} -> tcp://{SERVER_IP}:{VIDEO_TCP_PORT}")

    retry_delay = 1.0
    while not stop_event.is_set():
        if not ping_once(GATEWAY_IP):
            print("[SUP] Gateway ping failed; starting recovery...")
            bounce_interface(IFACE)
            waited = wait_for_gateway(GATEWAY_IP, timeout_s=25.0)
            if not waited:
                time.sleep(min(retry_delay, 5.0)); retry_delay = min(retry_delay * 2, 10.0); continue

        # Heartbeat (re)start via PayloadStatusSource
        if hb is None or not hb.is_running() or hb.has_fault():
            try:
                if hb: hb.stop()
            except Exception: pass
            hb = PayloadStatusSource(SERVER_IP, HEARTBEAT_PORT, interval_s=0.25, connect_timeout=5.0)
            hb.start()

        # CoT (re)connect if needed (lazy)
        if not cot_tcp.is_connected():
            try:
                cot_tcp.connect(retry=1, delay=1.0)
            except Exception:
                pass

        # (Re)start ffmpeg if needed
        if ff is None or ff.poll() is not None:
            if ff is not None:
                print(f"[SUP] FFmpeg died (code {ff.returncode}); will recover...")
            ff = launch_ffmpeg(SERVER_IP, VIDEO_TCP_PORT, FRAME_RATE, FRAME_WIDTH, FRAME_HEIGHT, V4L2_DEVICE, INPUT_FORMAT)

        # Basic pacing + light monitoring
        for _ in range(10):  # ~2.5s total
            if stop_event.is_set(): clean_exit()
            if hb and hb.has_fault():
                print("[SUP] Heartbeat fault detected; initiating recovery..."); break
            if ff and ff.poll() is not None:
                print(f"[SUP] FFmpeg exited (code {ff.returncode}); restarting..."); break
            time.sleep(0.25)

        # If fault, bounce and try again
        if hb and hb.has_fault():
            try: hb.stop()
            except Exception: pass
            hb = None
            stop_ffmpeg(ff); ff = None
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
