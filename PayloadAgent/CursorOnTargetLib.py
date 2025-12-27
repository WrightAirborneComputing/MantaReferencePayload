# ---------- Cursor-on-Target bridge (UDP) ----------
from xml.etree import ElementTree as ET  # noqa: E402
import time
import math
from datetime import datetime, timedelta, timezone
from UdpConnectionLib    import UdpSender

class CursorOnTargetBridge:
    """
    Convert MAVLink GLOBAL_POSITION_INT to Cursor-on-Target (CoT) and send over UDP.
    """
    def __init__(self, uid, cot_type, udp_sender, min_interval, callsign, geoid_height_fn):
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

