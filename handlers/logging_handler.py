"""This module contains the logging setup for the application."""
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
    """
    Custom logging handler for displaying log messages in a QTextEdit widget,
    while also printing them to stdout or stderr.
    """
    # signal for emitting log messages to the GUI
    log_signal = pyqtSignal(str)

    def __init__(self, text_edit: QTextEdit):
        """
        Initialize the handler with a QTextEdit widget.
        :param text_edit: QTextEdit widget for displaying log messages
        """
        logging.Handler.__init__(self)
        QObject.__init__(self)
        self.te = text_edit
        self.log_signal.connect(self.append_to_text_edit)

    def emit(self, record: logging.LogRecord, flushed=False):
        """
        Emit a log message through the log_signal and print it to stdout or stderr.
        :param record: LogRecord object
        :param flushed: bool indicating whether the message has been already printed to stdout, default is False,
                        (used during flushing from initialization logger)
        """
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
        """
        Append a message to the QTextEdit widget. Called by the log_signal.
        :param message: A log message to append to the QTextEdit widget
        """
        self.te.append(message)
        self.te.ensureCursorVisible()


class InitLogHandler(logging.Handler):
    """
    Custom logging handler for buffering log messages during application initialization,
    while also printing them to stdout or stderr.
    """
    def __init__(self):
        """Initialize the handler with an empty buffer."""
        super().__init__()
        self.buffer = []

    def emit(self, record):
        """
        Emit a log message and print it to stdout or stderr.
        :param record: LogRecord object
        """
        self.buffer.append(record)
        msg = self.format(record)
        # print DEBUG, INFO, WARNING to stdout, while ERROR and CRITICAL to stderr
        if record.levelno < logging.ERROR:
            print(msg, file=sys.stdout)
        else:
            print(msg, file=sys.stderr)

    def flush_to_qt_handler(self, handler: QtLogHandler):
        """
        Flush the buffer to a QtLogHandler.
        :param handler: QtLogHandler object into flush the buffer to
        """
        for record in self.buffer:
            handler.emit(record, flushed=True)
        self.buffer = []


# work with app logger only (maybe external modules can log somewhere else one day...)
logger = logging.getLogger("telcorain")


def setup_qt_logging(text_edit: QTextEdit, init_logger: InitLogHandler, log_level: Union[str, int] ) -> QtLogHandler:
    """
    Set up the Qt logging handler for the application.
    :param text_edit: QTextEdit widget for displaying log messages
    :param init_logger: InitLogHandler object for flushing the initialization log messages
    :param log_level: log level with which to set the Qt logger
    :return: QtLogHandler object
    """
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
    """
    Set up the initialization logging handler for the application.
    :return: InitLogHandler object
    """
    init_logger = InitLogHandler()
    init_formatter = logging.Formatter(fmt='[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
    init_formatter.converter = time.gmtime  # use UTC time
    init_logger.setFormatter(init_formatter)
    logger.addHandler(init_logger)
    logger.setLevel(config_manager.read_option('logging', 'init_level'))
    return init_logger


def setup_file_logging():
    """Set up the file logging for the application."""
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
