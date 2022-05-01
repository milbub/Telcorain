from PyQt6.QtWidgets import QApplication
import gui.main_window as mw
import sys


if __name__ == '__main__':
    app = QApplication(sys.argv)  # currently, there are no CLI args
    # Qt's main window = central hub of the application
    # constructor of the main window contains all the starting mechanism -> see /gui/main_window.py
    main_win = mw.MainWindow()

    main_win.statusBar().showMessage("Application is ready.", 20000)
    print("Starting done. Application is ready.", flush=True)

    sys.exit(app.exec())
