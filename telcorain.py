from PyQt6.QtWidgets import QApplication
import gui.main_window as gui_main
import sys

if __name__ == '__main__':
    app = QApplication(sys.argv)
    gui = gui_main.MainWindow()
    gui.statusBar().showMessage("Telcorain has started!", 20000)
    sys.exit(app.exec())
