# --------------- Utilities ------------------
from datetime import datetime

def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# def

def safe_preview(b: bytes, max_len: int = 200) -> str:
    try:
        s = b.decode("utf-8", errors="replace")
    except Exception:
        s = "<decode error>"
    if len(s) > max_len:
        s = s[:max_len] + f"...(+{len(s)-max_len} more)"
    return s.replace("\r", "\\r").replace("\n", "\\n").replace("\t", "\\t")
# def
