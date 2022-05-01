import sys
import time


class Logger:
    def __init__(self, text_edit):
        self.out = sys.__stdout__
        self.te = text_edit

    # override stdout write method
    def write(self, message):
        # if not control character, print to GUI
        if message != ("\n" or "\r"):
            current_time = time.strftime("[%H:%M:%S] ", time.localtime())
            message = current_time + message
            self.te.append(message)

        # write also to default stdout
        self.out.write(message)

    # override stdout flush method
    def flush(self):
        self.out.flush()
