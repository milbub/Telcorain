"""
Loading screen for Telcorain application.
This is independent logic of the main application and should be loaded via subprocess.Popen() as separate process.
"""
import sys

from PyQt6.QtGui import QIcon, QPixmap
from PyQt6.QtWidgets import QApplication, QLabel, QWidget, QVBoxLayout
from PyQt6.QtCore import Qt

app = QApplication(sys.argv)


class LoadingScreen(QWidget):
    """Loading screen for Telcorain application."""
    def __init__(self):
        super().__init__()
        self.setWindowFlags(Qt.WindowType.FramelessWindowHint | Qt.WindowType.WindowStaysOnTopHint)
        self.setStyleSheet("background-color: #FFFFFF;")
        self.setWindowTitle("Loading Telcorain")
        self.setWindowIcon(QIcon("./app/gui/icons/app_96x96.png"))

        layout = QVBoxLayout()

        # set up image
        self.image_label = QLabel(self)
        pixmap = QPixmap("./app/gui/icons/app_96x96.png")
        self.image_label.setPixmap(pixmap)
        self.image_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.image_label.setStyleSheet("margin: 15px;")
        layout.addWidget(self.image_label)

        # set up text
        self.label = QLabel("Loading Telcorain, please wait...")
        self.label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.label.setStyleSheet("font-weight: bold;")  # Make the text bold
        layout.addWidget(self.label)

        self.setLayout(layout)
        self.setFixedSize(300, 200)


loading_screen = LoadingScreen()
loading_screen.show()

sys.exit(app.exec())
