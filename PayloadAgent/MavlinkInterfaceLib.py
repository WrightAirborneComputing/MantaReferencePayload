# ---------- MAVLink interface ----------
import time
from pymavlink import mavutil
from typing import Optional, Dict

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

    def open(self):
        print(f"Opening {self.device} at {self.baud} baud (RX on GPIO15)")
        self.master = mavutil.mavlink_connection(
            self.device, baud=self.baud,
            autoreconnect=self.autoreconnect,
            source_system=self.source_system
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
                m.target_system or 1, m.target_component or 1,
                mavutil.mavlink.MAV_CMD_SET_MESSAGE_INTERVAL, 0,
                msgid, interval_us, 0, 0, 0, 0, 0
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

    def _handle_message(self, msg: mavutil.mavlink.MAVLink_message, count: int):
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

