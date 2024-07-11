from datetime import datetime
import logging
import os
import sys
import time
from typing import Union

from PyQt6.QtCore import QObject, pyqtSignal
from PyQt6.QtWidgets import QTextEdit

from handlers import config_manager


class QtLogHandler(logging.Handler, QObject):
    # signal for emitting log messages to the GUI
    log_signal = pyqtSignal(str)

    def __init__(self, text_edit: QTextEdit):
        logging.Handler.__init__(self)
        QObject.__init__(self)
        self.te = text_edit
        self.log_signal.connect(self.append_to_text_edit)

    def emit(self, record: logging.LogRecord, flushed=False):
        msg = self.format(record)
        self.log_signal.emit(msg)
        # if flushing, message has been already printed to stdout, no need to print it again
        if flushed:
            return

        # print DEBUG, INFO, WARNING to stdout, while ERROR and CRITICAL to stderr
        if record.levelno < logging.ERROR:
            print(msg, file=sys.stdout)
        else:
            print(msg, file=sys.stderr)

    def append_to_text_edit(self, message: str):
        self.te.append(message)
        self.te.ensureCursorVisible()


class InitLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.buffer = []

    def emit(self, record):
        self.buffer.append(record)
        msg = self.format(record)
        # print DEBUG, INFO, WARNING to stdout, while ERROR and CRITICAL to stderr
        if record.levelno < logging.ERROR:
            print(msg, file=sys.stdout)
        else:
            print(msg, file=sys.stderr)

    def flush_to_qt_handler(self, handler: QtLogHandler):
        for record in self.buffer:
            handler.emit(record, flushed=True)
        self.buffer = []


# work with app logger only (maybe external modules can log somewhere else one day...)
logger = logging.getLogger("telcorain")


def setup_qt_logging(text_edit: QTextEdit, init_logger: InitLogHandler, log_level: Union[str, int] ) -> QtLogHandler:
    qt_logger = QtLogHandler(text_edit)
    qt_formatter = logging.Formatter(fmt='[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
    qt_formatter.converter = time.gmtime  # use UTC time
    qt_logger.setFormatter(qt_formatter)
    logger.addHandler(qt_logger)

    # flush the init logger to the qt logger and remove it
    init_logger.flush_to_qt_handler(qt_logger)
    logger.removeHandler(init_logger)

    logger.setLevel(log_level)
    return qt_logger


def setup_init_logging() -> InitLogHandler:
    init_logger = InitLogHandler()
    init_formatter = logging.Formatter(fmt='[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
    init_formatter.converter = time.gmtime  # use UTC time
    init_logger.setFormatter(init_formatter)
    logger.addHandler(init_logger)
    logger.setLevel(config_manager.read_option('logging', 'init_level'))
    return init_logger


def setup_file_logging():
    logs_dir = config_manager.read_option('directories', 'logs')
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    start_time = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
    log_filename = f'{logs_dir}/{start_time}.log'
    file_handler = logging.FileHandler(log_filename)
    file_formatter = logging.Formatter(fmt='[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_formatter.converter = time.gmtime  # use UTC time
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
