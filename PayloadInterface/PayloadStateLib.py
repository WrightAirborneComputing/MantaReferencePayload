import threading

class PayloadState:
    """
    Stores the last peer IP we saw on status, so we can send commands back there.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self._hb_peer_ip = None

    def set_hb_peer(self, ip):
        with self._lock:
            self._hb_peer_ip = ip

    def get_hb_peer(self):
        with self._lock:
            return self._hb_peer_ip

