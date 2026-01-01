# ---------- MAVLink interface ----------
import json
import math
import os
import threading
import time
from types import SimpleNamespace
from typing import Optional, Dict, Tuple

from pymavlink import mavutil


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

# Where we persist the last known valid GPS fix
LAST_GPS_FILE = "/home/pi/ReferencePayload/last_gps_fix.json"


class MavlinkInterface:
    def __init__(self, device, baud, cot_bridge):
        self.device = device
        self.baud = baud
        self.req_intervals_hz = dict(DEFAULT_REQ_INTERVALS_HZ)
        self.source_system = 255
        self.autoreconnect = True
        self.master: Optional[mavutil.mavfile] = None
        self._running = False
        self.cot_bridge = cot_bridge

        # ---- store latest pitch (radians) from ATTITUDE ----
        self._att_lock = threading.Lock()
        self._pitch_rad: Optional[float] = None
        self._pitch_ts: float = 0.0  # time.time() of last update

        # ---- last valid GPS fix (lat/lon in degrees, alt_m in meters) ----
        self._gps_lock = threading.Lock()
        self._last_fix: Tuple[float, float, float] = self._load_or_init_last_fix()

    # ---- pitch getters ----
    def get_pitch_rad(self) -> Optional[float]:
        """Latest pitch in radians (ATTITUDE.pitch). None if not received yet."""
        with self._att_lock:
            return self._pitch_rad

    def get_pitch_deg(self) -> Optional[float]:
        """Latest pitch in degrees. None if not received yet."""
        with self._att_lock:
            if self._pitch_rad is None:
                return None
            return math.degrees(self._pitch_rad)

    def get_pitch_age_s(self) -> Optional[float]:
        """Seconds since last pitch update. None if never received."""
        with self._att_lock:
            if self._pitch_ts <= 0.0:
                return None
            return max(0.0, time.time() - self._pitch_ts)

    # ---- GPS persistence helpers ----
    def _load_or_init_last_fix(self) -> Tuple[float, float, float]:
        """
        Loads last_gps_fix.json if present and valid; otherwise creates it as 0,0,0.
        Returns (lat_deg, lon_deg, alt_m).
        """
        fix = self._read_fix_file()
        if fix is not None:
            return fix

        # No file or invalid -> create 0/0
        fix0 = (0.0, 0.0, 0.0)
        self._write_fix_file(*fix0)
        return fix0

    def _read_fix_file(self) -> Optional[Tuple[float, float, float]]:
        try:
            if not os.path.exists(LAST_GPS_FILE):
                return None
            with open(LAST_GPS_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            lat = float(d.get("lat", 0.0))
            lon = float(d.get("lon", 0.0))
            alt = float(d.get("alt_m", 0.0))

            if not self._is_plausible_latlon(lat, lon):
                return None
            return (lat, lon, alt)
        except Exception:
            return None

    def _write_fix_file(self, lat_deg: float, lon_deg: float, alt_m: float) -> None:
        try:
            tmp = LAST_GPS_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"lat": lat_deg, "lon": lon_deg, "alt_m": alt_m, "ts": time.time()}, f)
            os.replace(tmp, LAST_GPS_FILE)
        except Exception:
            # Best-effort only
            pass

    @staticmethod
    def _is_plausible_latlon(lat_deg: float, lon_deg: float) -> bool:
        if not (-90.0 <= lat_deg <= 90.0 and -180.0 <= lon_deg <= 180.0):
            return False
        # treat exactly 0/0 as "not a valid GPS" for runtime, but allowed for file init
        return True

    @staticmethod
    def _is_valid_runtime_fix(lat_deg: float, lon_deg: float, sats: int) -> bool:
        # Decide whether an incoming message has a real fix:
        if (sats < 6):
            return False
        elif not (-90.0 <= lat_deg <= 90.0 and -180.0 <= lon_deg <= 180.0):
            return False
        elif abs(lat_deg) < 1e-9 and abs(lon_deg) < 1e-9:
            return False
        return True

    def _update_last_fix(self, lat_deg: float, lon_deg: float, alt_m: float, source: str) -> None:
        with self._gps_lock:
            self._last_fix = (lat_deg, lon_deg, alt_m)
            self._write_fix_file(lat_deg, lon_deg, alt_m)
        # Light logging
        print(f"[GPS] Stored last fix from {source}: lat={lat_deg:.7f} lon={lon_deg:.7f} alt={alt_m:.1f}m")

    def _get_last_fix(self) -> Tuple[float, float, float]:
        return self._read_fix_file()

    # ---- mavlink plumbing ----
    def open(self):
        print(f"Opening {self.device} at {self.baud} baud (RX on GPIO15)")
        self.master = mavutil.mavlink_connection(
            self.device,
            baud=self.baud,
            autoreconnect=self.autoreconnect,
            source_system=self.source_system,
        )

    def wait_heartbeat(self, timeout):
        print("Waiting for HEARTBEAT...")
        hb = self.master.wait_heartbeat(timeout=timeout)
        if hb:
            print(f"Heartbeat from sys={hb.get_srcSystem()} comp={hb.get_srcComponent()}")
        else:
            print("No heartbeat detected. Continuing to listen.")
        return hb

    @staticmethod
    def resolve_msg_id(name):
        try:
            return mavutil.mavlink.map_name_to_id(name)
        except Exception:
            pass
        const_name = "MAVLINK_MSG_ID_" + name
        return getattr(mavutil.mavlink, const_name, None)

    def set_message_intervals(self):
        m = self.master
        for name, hz in self.req_intervals_hz.items():
            msgid = self.resolve_msg_id(name)
            if msgid is None:
                print("Skip unknown message name:", name)
                continue
            interval_us = int(1_000_000 / hz) if hz > 0 else -1
            m.mav.command_long_send(
                m.target_system or 1,
                m.target_component or 1,
                mavutil.mavlink.MAV_CMD_SET_MESSAGE_INTERVAL,
                0,
                msgid,
                interval_us,
                0,
                0,
                0,
                0,
                0,
            )
            time.sleep(0.02)
        print("Requested per-message intervals.")

    def request_legacy_streams(self, rate_hz: int = 5):
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

    def listen(self):
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

    def stop(self):
        self._running = False

    # ---- CoT dispatch helper ----
    def _send_cot_using_msg_or_last_fix(self, msg, source: str) -> None:
        if not self.cot_bridge:
            return

        # Try to read lat/lon/alt from the msg in the usual MAVLink units
        lat_deg = None
        lon_deg = None
        alt_m = None

        mtype = msg.get_type()
        if mtype == "GPS_RAW_INT":
            lat_deg = getattr(msg, "lat", 0) / 1e7
            lon_deg = getattr(msg, "lon", 0) / 1e7
            alt_m = getattr(msg, "alt", 0) / 1000.0
            sats  = getattr(msg, "satellites_visible", 0)

            if lat_deg is not None and lon_deg is not None and self._is_valid_runtime_fix(lat_deg, lon_deg, sats):
                # Good fix: pass through original msg
                self.cot_bridge.process_global_position_int(msg)
                return

            # Bad/zero fix: use stored last fix (or 0/0 if that's all we have)
            last_lat, last_lon, last_alt = self._get_last_fix()

            # Build a small proxy that looks like GLOBAL_POSITION_INT for downstream code
            proxy = SimpleNamespace(
                lat=int(last_lat * 1e7),
                lon=int(last_lon * 1e7),
                alt=int(last_alt * 1000.0),
                vz=0,  # cm/s (unknown)
                get_type=lambda: "GPS_RAW_INT",
            )

            # Optional log so you know fallback is happening
            # print(f"[GPS] No valid fix in {source}; using stored fix lat={last_lat:.7f} lon={last_lon:.7f}")
            self.cot_bridge.process_global_position_int(proxy)
        # if
    # def

    def _handle_message(self, msg: mavutil.mavlink.MAVLink_message, count: int):
        mtype = msg.get_type()

        if mtype == "ATTITUDE":
            # ---- store pitch ----
            pitch = getattr(msg, "pitch", None)
            if pitch is not None:
                with self._att_lock:
                    self._pitch_rad = float(pitch)
                    self._pitch_ts = time.time()

        elif mtype == "GPS_RAW_INT":
            lat_deg = getattr(msg, "lat", 0) / 1e7
            lon_deg = getattr(msg, "lon", 0) / 1e7
            alt_m = getattr(msg, "alt", 0) / 1000.0
            fix = int(getattr(msg, "fix_type", 0) or 0)
            sats = int(getattr(msg, "satellites_visible", 0) or 0)

            print(f"[{count:06d}] GPS_RAW_INT fix={fix} lat={lat_deg:.7f} lon={lon_deg:.7f} sat={sats}")

            # Store only if:
            #  - valid runtime fix, AND
            #  - at least 6 satellites
            if self._is_valid_runtime_fix(lat_deg, lon_deg, sats):
                self._update_last_fix(lat_deg, lon_deg, alt_m, "GPS_RAW_INT")

            # Also acceptable source for CoT (with fallback)
            self._send_cot_using_msg_or_last_fix(msg, "GPS_RAW_INT")

        else:
            pass
