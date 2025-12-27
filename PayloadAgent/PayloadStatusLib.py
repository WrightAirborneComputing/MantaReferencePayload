# ---------- Status (UDP) ----------
from UdpConnectionLib    import UdpSender
import threading

class PayloadStatusSource:
    """
    Periodically sends a simple heartbeat line over UDP.
    """
    def __init__(self, dest_ip, port, interval_s) -> None:
        self.dest_ip = dest_ip
        self.port = int(port)
        self.interval_s = float(interval_s)

        self._udp = UdpSender(dest_ip, port)
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._fault = threading.Event()
        self._counter = 1
    # def

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._fault.clear()
        self._thread = threading.Thread(target=self._run, name="PayloadStatusSource", daemon=True)
        self._thread.start()
    # def

    def stop(self) -> None:
        self._stop.set()
        try:
            self._udp.send(b"STOPPING_STATUS\n", label="ST")
        except Exception:
            pass
        try:
            self._udp.close()
        except Exception:
            pass
        if self._thread:
            self._thread.join(timeout=1.5)
    # def

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()
    # def

    def has_fault(self) -> bool:
        return self._fault.is_set()
    # def

    def _run(self) -> None:
        try:
            self._udp.send(b"STARTING_STATUS\n", label="ST")
            print(f"[UDP STATUS] Sending to {self.dest_ip}:{self.port}")
        except Exception as e:
            print(f"[UDP STATUS] start failed: {e}")
            self._fault.set()
            return

        while not self._stop.is_set():
            try:
                msg = f"Test {self._counter}\n".encode("utf-8")
                self._udp.send(msg, label="STATUS")
                self._counter += 1
            except Exception as e:
                print(f"[UDP STATUS] send error: {e}")
                self._fault.set()
                break

            if self._stop.wait(self.interval_s):
                break
    # def

