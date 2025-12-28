#!/usr/bin/env python3
"""
PayloadAgent.py  (UDP version)

Side-A IP (receiver): 192.168.144.11
Gateway IP (reachability ping): 192.168.144.10
"""

import os
import sys
import time
import math
import signal
import shutil
import threading
import subprocess
from typing import Optional, Dict, Callable

from VideoStreamerLib    import VideoStreamer
from UdpConnectionLib    import UdpSender
from MavlinkInterfaceLib import MavlinkInterface
from CursorOnTargetLib   import CursorOnTargetBridge
from PayloadStatusLib    import PayloadStatusSource

# ---------- Network & device config ----------
SIDE_A_IP        = "192.168.144.11"      # Side-A receiver
GATEWAY_IP       = "192.168.144.10"      # Herelink Air Unit (reachability ping)
IFACE            = "eth0"                # interface to bounce on faults

VIDEO_CTRL_PORT  = 6000                  # UDP video control ONLY (rx only)
VIDEO_PORT       = 7000                  # UDP video (mpegts)
COT_PORT         = 8000                  # UDP CoT
STATUS_PORT      = 9000                  # UDP status

# ---------- Video settings (optimised for Pi but using MJPEG input) ----------
FRAME_WIDTH      = 1280
FRAME_HEIGHT     = 720
FRAME_RATE       = 8
BITRATE_BPS      = 2_000_000
V4L2_DEVICE      = "/dev/video0"
INPUT_FORMAT     = "mjpeg"

# ---------- MAVLink settings ----------
DEFAULT_MAVLINK_DEVICE   = "/dev/serial0"
DEFAULT_MAVLINK_BAUD     = 115200

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

# ---------- Supervisor: HB + Video + MAV/CoT ----------
def supervisor_main():
    print(
        f"Sending UDP heartbeat to {SIDE_A_IP}:{STATUS_PORT}, "
        f"UDP video to {SIDE_A_IP}:{VIDEO_PORT}, "
        f"UDP CoT to {SIDE_A_IP}:{COT_PORT}, "
        f"and listening for video control on local UDP:{VIDEO_CTRL_PORT} (RX-only)."
    )

    if not os.path.exists(V4L2_DEVICE):
        print(f"[VIDEO] Device {V4L2_DEVICE} not found. Is the webcam connected?")
        return

    stop_event = threading.Event()
    status_src = None

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
    mav_iface = MavlinkInterface(DEFAULT_MAVLINK_DEVICE, DEFAULT_MAVLINK_BAUD, cot_bridge)
    try:
        mav_iface.open()
    except Exception as e:
        print(f"[MAV] Could not open {dev}: {e}")
        print("Tip: add your user to 'dialout' group or run with sudo.")
    else:
        def mav_worker():
            try:
                mav_iface.wait_heartbeat(10.0)
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
            if status_src:
                status_src.stop()
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
    print(f"Heartbeat: Side-A {SIDE_A_IP}:{STATUS_PORT}")

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

        # Status (re)start (9000)
        if status_src is None or not status_src.is_running() or status_src.has_fault():
            try:
                if status_src:
                    status_src.stop()
            except Exception:
                pass
            status_src = PayloadStatusSource(SIDE_A_IP, STATUS_PORT, 1.0)
            status_src.start()

        # Ensure ffmpeg running
        video.ensure_running()

        # Basic pacing + light monitoring
        for _ in range(10):  # ~2.5s total
            if stop_event.is_set():
                clean_exit()
            if status_src and status_src.has_fault():
                print("[SUP] Heartbeat fault detected; initiating recovery...")
                break
            if video._ff is None or (video._ff is not None and video._ff.poll() is not None):
                print("[SUP] FFmpeg not running; restarting...")
                break
            time.sleep(0.25)

        # If HB fault, bounce and try again
        if status_src and status_src.has_fault():
            try:
                status_src.stop()
            except Exception:
                pass
            status_src = None
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
    supervisor_main()
