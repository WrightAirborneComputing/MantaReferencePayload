
import threading
import socket
import json
import time
from UtilsLib import ts,safe_preview

class VideoUDPReceiver:
    """
    Owns:
      - UDP socket bind + recv loop
      - pushes received MPEG-TS datagrams into a UDPBytePipe
    """
    def __init__(
        self, stop_event, label, pipe,
        listen_host, listen_port,max_dgram):

        self.stop_event = stop_event
        self.pipe = pipe
        self.listen_host = listen_host
        self.listen_port = int(listen_port)
        self.max_dgram = int(max_dgram)
        self.label = label

        self._thread: Optional[threading.Thread] = None
        self._sock: Optional[socket.socket] = None
    # def

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
    # def

    def stop(self):
        # stop_event is managed by the app; just help unblock I/O
        try:
            if self._sock is not None:
                self._sock.close()
        except Exception:
            pass
    # def

    def join(self, timeout: float = 1.0):
        if self._thread:
            self._thread.join(timeout=timeout)
    # def

    def _run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock = sock
        try:
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            except Exception:
                pass

            sock.bind((self.listen_host, self.listen_port))
        except Exception as e:
            print(f"[{ts()}] [{self.label}] Bind failed on {self.listen_host}:{self.listen_port}: {e}")
            try:
                sock.close()
            except Exception:
                pass
            self.pipe.close()
            return

        print(f"[{ts()}] [{self.label}] Listening on {self.listen_host}:{self.listen_port} (raw UDP). Feeding decoder...")
        sock.settimeout(0.5)

        first = True
        try:
            while not self.stop_event.is_set():
                try:
                    data, peer = sock.recvfrom(self.max_dgram)
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.stop_event.is_set():
                        break
                    print(f"[{ts()}] [{self.label}] recv error: {e}")
                    break

                if not data:
                    continue

                if first:
                    first = False
                    print(f"[{ts()}] [{self.label}] <<< first datagram {len(data)}B from {peer[0]}:{peer[1]}")

                self.pipe.push(data)
        finally:
            try:
                sock.close()
            except Exception:
                pass
            self.pipe.close()
            print(f"[{ts()}] [{self.label}] Stopped.")
    # def

# class

class VideoCommandSender:
    """
    Owns:
      - UDP socket
      - periodic send loop
      - destination IP learned from PeerState (last status/heartbeat peer)

    Sends JSON payload:
      {"pan": x, "tilt": y, "zoom": zoom}
    All values are floats in [-1, 1].
    """
    def __init__(
        self,
        stop_event, label,
        payload_state,
        local_bind_host, dest_port, interval_s
    ):
        self.stop_event = stop_event
        self.payload_state = payload_state
        self.local_bind_host = local_bind_host
        self.dest_port = int(dest_port)
        self.interval_s = float(interval_s)
        self.label = label

        self._thread = None
        self._sock = None

        # --- stored command state (thread-safe) ---
        self._lock = threading.Lock()
        self._pan: float = 0.0
        self._tilt: float = 0.0
        self._zoom: float = 0.0
        self._auto: bool = False

        # Optional: if you ever want immediate sending on updates
        self._dirty_event = threading.Event()

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        try:
            if self._sock is not None:
                self._sock.close()
        except Exception:
            pass

    def join(self, timeout: float = 1.0):
        if self._thread:
            self._thread.join(timeout=timeout)

    @staticmethod
    def _clamp(v: float) -> float:
        try:
            v = float(v)
        except Exception:
            return 0.0
        if v < -1.0:
            return -1.0
        if v > 1.0:
            return 1.0
        return v

    def set_axes(self, x, y):
        """Store pan/tilt in [-1, 1]."""
        with self._lock:
            self._pan = self._clamp(x)
            self._tilt = self._clamp(y)

        # Optional: trigger immediate send rather than waiting interval
        # self._dirty_event.set()

    def set_zoom(self, z):
        """Store zoom in [-1, 1]."""
        with self._lock:
            self._zoom = self._clamp(z)

        # Optional: trigger immediate send rather than waiting interval
        # self._dirty_event.set()

    def set_auto(self, a):
        """Store auto as bool."""
        with self._lock:
            self._auto = bool(a)

        # Optional: trigger immediate send rather than waiting interval
        # self._dirty_event.set()

    def _get_state(self):
        with self._lock:
            return self._pan, self._tilt, self._zoom, self._auto

    def _run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock = sock

        try:
            sock.bind((self.local_bind_host, 0))
        except Exception:
            pass

        print(f"[{ts()}] [{self.label}] Sender enabled. "
              f"Will transmit JSON commands to UDP :{self.dest_port} (dest IP learned from status).")

        try:
            while not self.stop_event.is_set():
                ip = self.payload_state.get_hb_peer()
                if not ip:
                    if self.stop_event.wait(0.25):
                        break
                    continue

                pan, tilt, zoom, auto = self._get_state()
                msg = {"pan": pan, "tilt": tilt, "zoom": zoom, "auto": auto}
                payload = (json.dumps(msg, separators=(",", ":")) + "\n").encode("utf-8", errors="replace")

                try:
                    sock.sendto(payload, (ip, self.dest_port))
                    print(f"[{ts()}] [{self.label}] >>>> {ip}:{self.dest_port}  "
                          f"{len(payload)}B [{safe_preview(payload)}]")
                except Exception as e:
                    print(f"[{ts()}] [{self.label}] send error to {ip}:{self.dest_port}: {e}")

                # Periodic send. If you want "send immediately on change",
                # replace this wait with a wait on _dirty_event with timeout.
                if self.stop_event.wait(self.interval_s):
                    break

        finally:
            try:
                sock.close()
            except Exception:
                pass
            print(f"[{ts()}] [{self.label}] Stopped.")

    # def

# class

