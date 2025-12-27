import threading
import socket

from UtilsLib import ts, safe_preview

class UDPTextListener:
    """
    Owns:
      - UDP socket bind + recv loop
      - prints datagrams
      - optionally updates PeerState with last peer IP
    """
    def __init__(
        self,stop_event,label,
        listen_host,listen_port,peer_state,max_dgram,
    ):
        self.stop_event = stop_event
        self.label = label
        self.listen_host = listen_host
        self.listen_port = int(listen_port)
        self.peer_state = peer_state
        self.max_dgram = int(max_dgram)

        self._thread: Optional[threading.Thread] = None
        self._sock: Optional[socket.socket] = None

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
            return

        print(f"[{ts()}] [{self.label}] Listening on {self.listen_host}:{self.listen_port}")
        sock.settimeout(0.5)

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

                if self.peer_state is not None:
                    self.peer_state.set_hb_peer(peer[0])

                print(f"[{ts()}] [{self.label}] >>>> {peer[0]}:{peer[1]}  {len(data)}B [{safe_preview(data)}]")
        finally:
            try:
                sock.close()
            except Exception:
                pass
            print(f"[{ts()}] [{self.label}] Stopped.")

