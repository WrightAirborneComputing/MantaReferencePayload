#!/usr/bin/env python3
"""
GUI H.264/MPEG-TS viewer with side-by-side console.

UPDATED PORTS (side-B):
- VIDEO CTRL OUT    UDP :6001
- VIDEO (mpegts)    UDP :7001
- COT (xml/text)    UDP :8001   ---> forwarded to TCP 127.0.0.1:18087
- STATUS (text)     UDP :9001
"""

import sys
import threading

# --- Third-party ---
import numpy as np
import cv2
from PyQt5 import QtCore, QtGui, QtWidgets

# Internal
from VideoLib import VideoUDPReceiver, VideoCommandSender
from DecoderThreadLib import DecoderThread
from ForwarderLib import UDPTCPForwarder
from TextListenerLib import UDPTextListener
from PayloadStateLib import PayloadState
from UtilsLib import ts

# ------------------ Config ------------------
VIDEO_CMD_UDP_PORT = 6001
VIDEO_UDP_PORT     = 7001
STATUS_UDP_PORT    = 9001
COT_UDP_PORT       = 8001

UDP_LISTEN_HOST = "192.168.43.74"

COT_TCP_HOST = "127.0.0.1"
COT_TCP_PORT = 18087

UDP_MAX_DGRAM = 65535

WINDOW_TITLE = "MPEG-TS Viewer + Console"
INITIAL_WIN_W, INITIAL_WIN_H = 1200, 700


# ---------- Qt console redirection ----------
class EmittingStream(QtCore.QObject):
    text_ready = QtCore.pyqtSignal(str)

    def write(self, text):
        if text:
            self.text_ready.emit(str(text))

    def flush(self):
        pass


class ConsoleWidget(QtWidgets.QTextEdit):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setReadOnly(True)
        self.setLineWrapMode(QtWidgets.QTextEdit.NoWrap)
        self.setFont(QtGui.QFont("Consolas" if sys.platform.startswith("win") else "Monospace", 10))
        pal = self.palette()
        pal.setColor(QtGui.QPalette.Base, QtGui.QColor(15, 15, 15))
        pal.setColor(QtGui.QPalette.Text, QtGui.QColor(220, 220, 220))
        self.setPalette(pal)

    @QtCore.pyqtSlot(str)
    def append_text(self, text: str):
        self.moveCursor(QtGui.QTextCursor.End)
        self.insertPlainText(text)
        self.moveCursor(QtGui.QTextCursor.End)


# -------------- Video display ---------------
class VideoWidget(QtWidgets.QLabel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAlignment(QtCore.Qt.AlignCenter)
        self.setMinimumSize(320, 240)
        self.setStyleSheet("QLabel { background-color: #000; }")

    def set_frame(self, frame_bgr: np.ndarray):
        if frame_bgr is None:
            return
        h, w = frame_bgr.shape[:2]
        rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
        qimg = QtGui.QImage(rgb.data, w, h, rgb.strides[0], QtGui.QImage.Format_RGB888)
        pix = QtGui.QPixmap.fromImage(qimg)
        self.setPixmap(pix.scaled(self.size(), QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation))

    def resizeEvent(self, event):
        pm = self.pixmap()
        if pm:
            self.setPixmap(pm.scaled(self.size(), QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation))
        super().resizeEvent(event)


# ---------- Video UDP -> file-like reader ----------
class UDPBytePipe:
    def __init__(self, stop_event: threading.Event):
        self._buf = bytearray()
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._stop_event = stop_event
        self._closed = False

    def close(self):
        with self._cv:
            self._closed = True
            self._cv.notify_all()

    def push(self, data: bytes):
        if not data:
            return
        with self._cv:
            self._buf.extend(data)
            self._cv.notify_all()

    def readable(self):
        return True

    def read(self, n: int) -> bytes:
        with self._cv:
            while not self._stop_event.is_set() and not self._closed and len(self._buf) == 0:
                self._cv.wait(timeout=0.5)

            if self._stop_event.is_set() or self._closed:
                return b""

            take = min(max(1, int(n)), len(self._buf))
            out = bytes(self._buf[:take])
            del self._buf[:take]
            return out


# -------------- Main window ----------------
class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(WINDOW_TITLE)
        self.resize(INITIAL_WIN_W, INITIAL_WIN_H)

        central = QtWidgets.QWidget(self)
        outer_vbox = QtWidgets.QVBoxLayout(central)
        outer_vbox.setContentsMargins(8, 8, 8, 8)
        outer_vbox.setSpacing(8)

        header = QtWidgets.QLabel(
            f"<b>Listening (side-B):</b> "
            f"Status UDP {UDP_LISTEN_HOST}:{STATUS_UDP_PORT} | "
            f"Video UDP {UDP_LISTEN_HOST}:{VIDEO_UDP_PORT} | "
            f"CoT UDP {UDP_LISTEN_HOST}:{COT_UDP_PORT} "
            f"&rarr; TCP {COT_TCP_HOST}:{COT_TCP_PORT} | "
            f"VideoCmd TX UDP :{VIDEO_CMD_UDP_PORT}"
        )
        outer_vbox.addWidget(header)

        splitter = QtWidgets.QSplitter(QtCore.Qt.Horizontal)

        # ---- Video group with sliders + auto checkbox ----
        video_group = QtWidgets.QGroupBox("Video (H.264 / MPEG-TS)")
        vg = QtWidgets.QGridLayout(video_group)
        vg.setContentsMargins(8, 8, 8, 8)
        vg.setHorizontalSpacing(6)
        vg.setVerticalSpacing(6)

        self.video_widget = VideoWidget()
        vg.addWidget(self.video_widget, 0, 0)

        # Right-hand slider column: TILT + ZOOM (two vertical sliders side-by-side)
        sliders_col = QtWidgets.QWidget()
        sliders_col_layout = QtWidgets.QHBoxLayout(sliders_col)
        sliders_col_layout.setContentsMargins(0, 0, 0, 0)
        sliders_col_layout.setSpacing(8)

        # --- Tilt group ---
        tilt_group = QtWidgets.QWidget()
        tilt_layout = QtWidgets.QVBoxLayout(tilt_group)
        tilt_layout.setContentsMargins(0, 0, 0, 0)
        tilt_layout.setSpacing(4)

        tilt_label = QtWidgets.QLabel("Tilt")
        tilt_label.setAlignment(QtCore.Qt.AlignHCenter)
        tilt_layout.addWidget(tilt_label)

        self.slider_y = QtWidgets.QSlider(QtCore.Qt.Vertical)
        self.slider_y.setRange(-1000, 1000)
        self.slider_y.setValue(0)
        self.slider_y.setTickPosition(QtWidgets.QSlider.TicksRight)
        self.slider_y.setTickInterval(250)
        self.slider_y.setToolTip("Tilt (Y): up = +1, down = -1")
        tilt_layout.addWidget(self.slider_y, stretch=1)

        sliders_col_layout.addWidget(tilt_group)

        # --- Zoom group ---
        zoom_group = QtWidgets.QWidget()
        zoom_layout = QtWidgets.QVBoxLayout(zoom_group)
        zoom_layout.setContentsMargins(0, 0, 0, 0)
        zoom_layout.setSpacing(4)

        zoom_label = QtWidgets.QLabel("Zoom")
        zoom_label.setAlignment(QtCore.Qt.AlignHCenter)
        zoom_layout.addWidget(zoom_label)

        self.slider_zoom = QtWidgets.QSlider(QtCore.Qt.Vertical)
        self.slider_zoom.setRange(-1000, 1000)
        self.slider_zoom.setValue(0)
        self.slider_zoom.setTickPosition(QtWidgets.QSlider.TicksRight)
        self.slider_zoom.setTickInterval(250)
        self.slider_zoom.setToolTip("Zoom: +1 / -1")
        zoom_layout.addWidget(self.slider_zoom, stretch=1)

        sliders_col_layout.addWidget(zoom_group)

        vg.addWidget(sliders_col, 0, 1)

        # Bottom horizontal slider (PAN)
        bottom_row = QtWidgets.QWidget()
        bottom_layout = QtWidgets.QVBoxLayout(bottom_row)
        bottom_layout.setContentsMargins(0, 0, 0, 0)
        bottom_layout.setSpacing(4)

        pan_label = QtWidgets.QLabel("Pan")
        pan_label.setAlignment(QtCore.Qt.AlignHCenter)
        bottom_layout.addWidget(pan_label)

        self.slider_x = QtWidgets.QSlider(QtCore.Qt.Horizontal)
        self.slider_x.setRange(-1000, 1000)
        self.slider_x.setValue(0)
        self.slider_x.setTickPosition(QtWidgets.QSlider.TicksBelow)
        self.slider_x.setTickInterval(250)
        self.slider_x.setToolTip("Pan (X): right = +1, left = -1")
        bottom_layout.addWidget(self.slider_x)

        # Readout + Auto checkbox (below sliders, same column as right controls)
        right_bottom = QtWidgets.QWidget()
        right_bottom_layout = QtWidgets.QVBoxLayout(right_bottom)
        right_bottom_layout.setContentsMargins(0, 0, 0, 0)
        right_bottom_layout.setSpacing(6)

        self.axes_readout = QtWidgets.QLabel("Pan=+0.00  Tlt=+0.00  Zm=+0.00  Auto=OFF")
        self.axes_readout.setAlignment(QtCore.Qt.AlignCenter)
        right_bottom_layout.addWidget(self.axes_readout)

        self.auto_checkbox = QtWidgets.QCheckBox("Auto")
        self.auto_checkbox.setToolTip("Enable/disable automatic mode in video control")
        self.auto_checkbox.setChecked(False)
        right_bottom_layout.addWidget(self.auto_checkbox, alignment=QtCore.Qt.AlignHCenter)

        vg.addWidget(bottom_row, 1, 0)
        vg.addWidget(right_bottom, 1, 1)

        # let video expand, sliders stay skinny
        vg.setColumnStretch(0, 1)
        vg.setColumnStretch(1, 0)
        vg.setRowStretch(0, 1)
        vg.setRowStretch(1, 0)

        splitter.addWidget(video_group)

        # ---- Console group ----
        console_group = QtWidgets.QGroupBox("Console (stdout / stderr)")
        console_layout = QtWidgets.QVBoxLayout(console_group)
        self.console = ConsoleWidget()
        console_layout.addWidget(self.console)
        splitter.addWidget(console_group)

        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 2)
        splitter.setSizes([int(INITIAL_WIN_W * 0.65), int(INITIAL_WIN_W * 0.35)])

        outer_vbox.addWidget(splitter, stretch=1)
        self.setCentralWidget(central)

        # Stop-signal shared across components
        self.stop_event = threading.Event()

        # Redirect stdout/stderr to UI
        self.stdout_stream = EmittingStream()
        self.stderr_stream = EmittingStream()
        self.stdout_stream.text_ready.connect(self.console.append_text)
        self.stderr_stream.text_ready.connect(self.console.append_text)
        sys.stdout = self.stdout_stream  # type: ignore
        sys.stderr = self.stderr_stream  # type: ignore

        # Shared peer state (learned from STATUS listener)
        self.payload_state = PayloadState()

        # --- Video pipeline: UDP receiver -> pipe -> decoder ---
        self.video_pipe = UDPBytePipe(self.stop_event)

        self.video_rx = VideoUDPReceiver(
            self.stop_event,
            "VIDEO",
            self.video_pipe,
            UDP_LISTEN_HOST,
            VIDEO_UDP_PORT,
            UDP_MAX_DGRAM,
        )
        self.video_rx.start()

        self.decoder = DecoderThread(
            self.video_pipe,
            self,
            f"MPEG-TS over UDP (bound {UDP_LISTEN_HOST}:{VIDEO_UDP_PORT})",
        )
        self.decoder.frame_ready.connect(self.video_widget.set_frame)
        self.decoder.start()

        # --- STATUS listener on :9001 (updates payload_state) ---
        self.status_listener = UDPTextListener(
            self.stop_event, "STATUS",
            UDP_LISTEN_HOST, STATUS_UDP_PORT,
            self.payload_state, UDP_MAX_DGRAM,
        )
        self.status_listener.start()

        # --- VIDEO COMMAND SENDER: driven by sliders/checkbox ---
        self.cmd_sender = VideoCommandSender(
            self.stop_event, "VIDCMD",
            self.payload_state,
            UDP_LISTEN_HOST, VIDEO_CMD_UDP_PORT,
            0.25,
        )
        self.cmd_sender.start()

        # Wire controls after cmd_sender exists
        self.slider_x.valueChanged.connect(self._on_axes_changed)
        self.slider_y.valueChanged.connect(self._on_axes_changed)
        self.slider_zoom.valueChanged.connect(self._on_zoom_changed)
        self.auto_checkbox.toggled.connect(self._on_auto_toggled)

        # push initial values
        self._on_axes_changed()
        self._on_zoom_changed()
        self._on_auto_toggled(self.auto_checkbox.isChecked())

        # --- COT forwarder: UDP :8001 -> TCP 127.0.0.1:18087 ---
        self.cot_forwarder = UDPTCPForwarder(
            self.stop_event, "COT",
            UDP_LISTEN_HOST, COT_UDP_PORT,
            COT_TCP_HOST, COT_TCP_PORT,
            UDP_MAX_DGRAM,
        )
        self.cot_forwarder.start()

        print(f"[{ts()}] UI ready. Close the window to quit.\n")

    def _update_readout(self):
        x = self.slider_x.value() / 1000.0
        y = -(self.slider_y.value() / 1000.0)  # invert Y
        z = self.slider_zoom.value() / 1000.0
        auto = self.auto_checkbox.isChecked()
        self.axes_readout.setText(
            f"Pan={x:+0.2f}  Tlt={y:+0.2f}  Zm={z:+0.2f}  Auto={'ON' if auto else 'OFF'}"
        )

    def _on_axes_changed(self):
        x = self.slider_x.value() / 1000.0
        y = -(self.slider_y.value() / 1000.0)
        self._update_readout()
        try:
            self.cmd_sender.set_axes(x, y)
        except Exception as e:
            print(f"[{ts()}] [UI] cmd_sender.set_axes failed: {e}")

    def _on_zoom_changed(self):
        z = self.slider_zoom.value() / 1000.0
        self._update_readout()
        try:
            self.cmd_sender.set_zoom(z)
        except Exception as e:
            print(f"[{ts()}] [UI] cmd_sender.set_zoom failed: {e}")

    def _on_auto_toggled(self, checked: bool):
        self._update_readout()
        # Expect VideoCommandSender to implement set_auto(bool)
        try:
            self.cmd_sender.set_auto(bool(checked))
        except Exception as e:
            print(f"[{ts()}] [UI] cmd_sender.set_auto failed: {e}")

    def closeEvent(self, event: QtGui.QCloseEvent):
        try:
            self.stop_event.set()

            try: self.video_rx.stop()
            except Exception: pass
            try: self.status_listener.stop()
            except Exception: pass
            try: self.cmd_sender.stop()
            except Exception: pass
            try: self.cot_forwarder.stop()
            except Exception: pass

            try: self.video_pipe.close()
            except Exception: pass

            if self.decoder.isRunning():
                self.decoder.stop()
                self.decoder.wait(2000)

            self.video_rx.join(1.0)
            self.status_listener.join(1.0)
            self.cmd_sender.join(1.0)
            self.cot_forwarder.join(1.0)

        finally:
            sys.stdout = sys.__stdout__
            sys.stderr = sys.__stderr__

        super().closeEvent(event)


# ------------------ Entry -------------------
def main():
    QtWidgets.QApplication.setAttribute(QtCore.Qt.AA_UseHighDpiPixmaps, True)
    app = QtWidgets.QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
