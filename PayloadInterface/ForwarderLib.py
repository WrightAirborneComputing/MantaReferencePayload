import socket
import threading
import time
import traceback
from typing import Optional

from UtilsLib import ts, safe_preview


class UDPTCPForwarder:
    """
    Owns:
      - UDP listener socket
      - TCP client connection with auto-reconnect
      - forwards each UDP datagram to TCP via sendall()
    """

    def __init__(
        self,
        stop_event,label,
        udp_host,udp_port,tcp_host,tcp_port,max_dgram
    ):
        self.stop_event = stop_event
        self.label = label

        self.udp_host = udp_host
        self.udp_port = int(udp_port)
        self.tcp_host = tcp_host
        self.tcp_port = int(tcp_port)

        self.max_dgram = int(max_dgram)

        self._thread: Optional[threading.Thread] = None
        self._udp_sock: Optional[socket.socket] = None
        self._tcp_sock: Optional[socket.socket] = None
        self._last_connect_log = 0.0
        self._tcp_lock = threading.Lock()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        # stop_event is owned by the app; here we just unblock I/O quickly
        try:
            if self._udp_sock is not None:
                self._udp_sock.close()
        except Exception:
            pass
        self._tcp_close()

    def join(self, timeout: float = 1.0) -> None:
        if self._thread:
            self._thread.join(timeout=timeout)

    def _tcp_close(self) -> None:
        with self._tcp_lock:
            if self._tcp_sock is not None:
                try:
                    self._tcp_sock.close()
                except Exception:
                    pass
            self._tcp_sock = None

    def _tcp_ensure_connected(self) -> bool:
        with self._tcp_lock:
            if self._tcp_sock is not None:
                return True

            now = time.time()
            if now - self._last_connect_log > 1.0:
                print(f"[{ts()}] [{self.label}] TCP connecting to {self.tcp_host}:{self.tcp_port}...")
                self._last_connect_log = now

            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2.0)
            try:
                s.connect((self.tcp_host, self.tcp_port))
            except Exception:
                try:
                    s.close()
                except Exception:
                    pass
                return False

            try:
                s.settimeout(None)
            except Exception:
                pass

            self._tcp_sock = s
            print(f"[{ts()}] [{self.label}] TCP connected to {self.tcp_host}:{self.tcp_port}")
            return True

    def _tcp_sendall(self, data: bytes) -> bool:
        # returns True if sent, False if failed
        with self._tcp_lock:
            s = self._tcp_sock
        if s is None:
            return False
        try:
            s.sendall(data)
            return True
        except Exception:
            return False

    def _run(self) -> None:
        try:
            udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._udp_sock = udp_sock

            try:
                try:
                    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                except Exception:
                    pass
                udp_sock.bind((self.udp_host, self.udp_port))
            except Exception as e:
                print(f"[{ts()}] [{self.label}] UDP bind failed on {self.udp_host}:{self.udp_port}: {e}")
                try:
                    udp_sock.close()
                except Exception:
                    pass
                return

            udp_sock.settimeout(0.5)
            print(
                f"[{ts()}] [{self.label}] Listening on UDP {self.udp_host}:{self.udp_port} "
                f"and forwarding to TCP {self.tcp_host}:{self.tcp_port}"
            )

            while not self.stop_event.is_set():
                try:
                    data, peer = udp_sock.recvfrom(self.max_dgram)
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.stop_event.is_set():
                        break
                    print(f"[{ts()}] [{self.label}] UDP recv error: {e}")
                    break

                if not data:
                    continue

                print(f"[{ts()}] [{self.label}] >>>> {peer[0]}:{peer[1]}  {len(data)}B [{safe_preview(data)}]")

                if not self._tcp_ensure_connected():
                    print(f"[{ts()}] [{self.label}] TCP not connected; dropped {len(data)}B")
                    continue

                if self._tcp_sendall(data):
                    continue

                # send failed -> reconnect + retry once
                print(f"[{ts()}] [{self.label}] TCP send failed; reconnecting...")
                self._tcp_close()
                if self._tcp_ensure_connected() and self._tcp_sendall(data):
                    continue

                print(f"[{ts()}] [{self.label}] TCP re-send failed; dropped {len(data)}B")
                self._tcp_close()

        except Exception:
            print(f"[{ts()}] [{self.label}] Forwarder thread crashed:\n{traceback.format_exc()}")

        finally:
            self._tcp_close()
            try:
                if self._udp_sock is not None:
                    self._udp_sock.close()
            except Exception:
                pass
            self._udp_sock = None
            print(f"[{ts()}] [{self.label}] Stopped.")
