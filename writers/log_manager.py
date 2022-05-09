import sys
import time

from PyQt6.QtCore import QTimer


class LogManager:
    def __init__(self, text_edit):
        self.out = sys.__stdout__
        self.te = text_edit

        self.buffer = ""

        # init timer for writing into gui from buffer in 1s interval
        self.write_timer = QTimer()
        self.write_timer.timeout.connect(self._flush_gui)
        self.write_timer.start(1000)

    # override stdout write method
    def write(self, message):
        # if not control character, write to buffer
        if message != ("\n" or "\r"):
            current_time = time.strftime("[%H:%M:%S] ", time.localtime())
            message = current_time + message

            if self.buffer == "":
                self.buffer += message
            else:
                self.buffer += '\n' + message

        # write to default stdout immediately
        self.out.write(message)

    # override stdout flush method
    def flush(self):
        self.out.flush()

    # empty buffer and print all in gui
    def _flush_gui(self):
        if self.buffer != "":
            self.te.append(self.buffer)
            self.buffer = ""
            # scroll to end
            self.te.ensureCursorVisible()
