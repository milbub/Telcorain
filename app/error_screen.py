"""
Show error screen in case of fatal error during start of Telcorain.
This is independent logic of the main application and should be loaded via subprocess.Popen() as separate process.
"""
import sys

from PyQt6.QtWidgets import QApplication
from PyQt6.QtCore import QCoreApplication

from app.utils import ErrorBox


app = QApplication(sys.argv)

# possible args: [1]: title, [2]: error message
if len(QCoreApplication.arguments()) < 3:
    title = "Error"
    error = "Unknown error occurred."
else:
    title = QCoreApplication.arguments()[1]
    error = QCoreApplication.arguments()[2]

# show the error (QMessageBox)
error_screen = ErrorBox(title, error)

sys.exit(app.exec())
