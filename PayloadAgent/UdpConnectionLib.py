# ---------- UDP sender + instrumentation ----------
import socket
import threading
import time

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

