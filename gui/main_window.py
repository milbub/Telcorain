from PyQt6.QtGui import QPixmap
from PyQt6.QtWidgets import QMainWindow, QLabel, QProgressBar, QSpacerItem, QSizePolicy, QHBoxLayout, QWidget
from PyQt6 import uic, QtGui, QtCore


class MainWindow(QMainWindow):
    def __init__(self):
        super(MainWindow, self).__init__()

        # set up window icon
        self.app_icon = QtGui.QIcon()
        self.app_icon.addFile('./gui/icons/app_16x16.png', QtCore.QSize(16, 16))
        self.app_icon.addFile('./gui/icons/app_32x32.png', QtCore.QSize(32, 32))
        self.app_icon.addFile('./gui/icons/app_96x96.png', QtCore.QSize(48, 48))
        self.setWindowIcon(self.app_icon)

        # load UI definition from Qt XML file
        uic.loadUi("./gui/MainWindow.ui", self)

        # set up statusbar
        self.status_db = QWidget()
        self.status_db_layout = QHBoxLayout()
        self.status_db_layout.setSpacing(0)
        self.status_db.setLayout(self.status_db_layout)
        self.status_db_label = QLabel()
        self.status_db_label.setText("InfluxDB:")
        self.status_db_layout.addWidget(self.status_db_label)
        self.status_db_icon = QPixmap('./gui/icons/cross_red.png')
        self.status_db_icon_lbl = QLabel()
        self.status_db_icon_lbl.setPixmap(self.status_db_icon)
        self.status_db_layout.addWidget(self.status_db_icon_lbl)
        self.status_db_state = QLabel()
        self.status_db_state.setText("Disconnected")
        self.status_db_layout.addWidget(self.status_db_state)
        self.statusBar().addPermanentWidget(self.status_db)
        self.status_prg = QWidget()
        self.status_prg_layout = QHBoxLayout()
        self.status_prg.setLayout(self.status_prg_layout)
        self.status_prg.setFixedWidth(220)
        self.status_prg_bar = QProgressBar()
        self.status_prg_bar.setFixedWidth(200)
        self.status_prg_bar.setTextVisible(False)
        self.status_prg_layout.addWidget(self.status_prg_bar)
        self.statusBar().addPermanentWidget(self.status_prg)
        self.statusBar().setContentsMargins(14, 0, 14, 0)

        # lookup for used widgets and define them

        # show window
        self.show()
