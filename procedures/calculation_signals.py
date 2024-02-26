from PyQt6.QtCore import QObject, pyqtSignal


class CalcSignals(QObject):
    overall_done_signal = pyqtSignal(dict)
    plots_done_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(dict)
    progress_signal = pyqtSignal(dict)
