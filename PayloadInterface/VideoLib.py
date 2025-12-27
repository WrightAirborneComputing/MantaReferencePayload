
from typing import Optional
import threading
import socket
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

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        # stop_event is managed by the app; just help unblock I/O
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

class VideoCommandSender:
    """
    Owns:
      - UDP socket
      - periodic send loop
      - destination IP learned from PeerState (last status/heartbeat peer)
    """
    def __init__(
        self,
        stop_event,label,
        payload_state,
        local_bind_host,dest_port,interval_s
    ):
        self.stop_event = stop_event
        self.payload_state = payload_state
        self.local_bind_host = local_bind_host
        self.dest_port = int(dest_port)
        self.interval_s = float(interval_s)
        self.label = label

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
            sock.bind((self.local_bind_host, 0))
        except Exception:
            pass

        counter = 0
        print(f"[{ts()}] [{self.label}] Sender enabled. Will transmit example commands to UDP :{self.dest_port} (dest IP learned from status).")

        try:
            while not self.stop_event.is_set():
                ip = self.payload_state.get_hb_peer()
                if not ip:
                    if self.stop_event.wait(0.5):
                        break
                    continue

                counter += 1
                payload = f"VIDEO_CMD #{counter}".encode("utf-8", errors="replace")

                try:
                    sock.sendto(payload, (ip, self.dest_port))
                    print(f"[{ts()}] [{self.label}] <<<< {ip}:{self.dest_port}  {len(payload)}B [{safe_preview(payload)}]")
                except Exception as e:
                    print(f"[{ts()}] [{self.label}] send error to {ip}:{self.dest_port}: {e}")

                if self.stop_event.wait(self.interval_s):
                    break
        finally:
            try:
                sock.close()
            except Exception:
                pass
            print(f"[{ts()}] [{self.label}] Stopped.")


