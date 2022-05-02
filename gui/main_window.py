import sys

from PyQt6 import uic, QtGui, QtCore
from PyQt6.QtCore import QTimer
from PyQt6.QtGui import QPixmap
from PyQt6.QtWidgets import QMainWindow, QLabel, QProgressBar, QHBoxLayout, QWidget, QTextEdit, QListWidget, \
    QDateTimeEdit, QPushButton, QSpinBox, QTabWidget, QLineEdit
from gui.results_widget import ResultsWidget

import input.influx_manager as influx
import input.sqlite_manager as sqlite
import procedures.calculation as calc
import writers.logger as logger


# TODO: move Control Tab elements into separate widget. Currently, this class contains main logic + Control Tab widgets.

class MainWindow(QMainWindow):
    def __init__(self):
        super(MainWindow, self).__init__()

        # ////// START GUI CONSTRUCTOR \\\\\\

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
        self.status_db_icon_lbl = QLabel()
        self.status_db_icon_lbl.setPixmap(QPixmap('./gui/icons/cross_red.png'))
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
        self.text_log = self.findChild(QTextEdit, "textLog")
        self.lists = self.findChild(QListWidget, "listLists")
        self.datetime_start = self.findChild(QDateTimeEdit, "dateTimeStart")
        self.datetime_stop = self.findChild(QDateTimeEdit, "dateTimeStop")
        self.spin_timestep = self.findChild(QSpinBox, "spinTimestep")
        self.butt_start = self.findChild(QPushButton, "buttStart")
        self.tabs = self.findChild(QTabWidget, "tabWidget")
        self.results_name = self.findChild(QLineEdit, "resultsNameEdit")

        # declare dictionary for created tabs with calculation results
        # <key: int = result ID, value: ResultsWidget>
        self.results_tabs = {}

        # results tabs ID counter
        self.result_id = 0

        # icon for results tabs
        self.results_icon = QtGui.QIcon()
        self.results_icon.addFile('./gui/icons/explore.png', QtCore.QSize(16, 16))

        # add default value to list of link's list (ALL = list of all links)
        self.lists.addItem("<ALL>")

        # connect buttons
        self.butt_start.clicked.connect(self.calculation_start)

        # show window
        self.show()

        # \\\\\\ END GUI CONSTRUCTOR //////

        # ////// START APP LOGIC CONSTRUCTOR \\\\\\

        # redirect stdout to log handler
        sys.stdout = logger.Logger(self.text_log)
        print("Telcorain is starting...", flush=True)

        # init threadpool
        self.threadpool = QtCore.QThreadPool()

        # init signaling
        self.influx_signals = influx.InfluxSignals()
        self.influx_signals.ping_signal.connect(self.check_influx_status)
        self.calc_signals = calc.CalcSignals()
        self.calc_signals.done_signal.connect(self.show_results)
        self.calc_signals.error_signal.connect(self.calculation_error)

        # init influxDB connection and status checker
        self.influx_status: int = 0  # 0 = unknown, 1 = ok, -1 = not available
        influx.InfluxChecker(self.influx_signals).run()  # first connection check

        self.influx_timer = QTimer()  # create timer for next checks
        self.influx_timer.timeout.connect(self._pool_checker)
        # TODO: load influx timeout from config and add some time
        self.influx_timer.start(5000)

        # load CML definitions from SQLite database
        self.sqlite_man = sqlite.SqliteManager()
        self.links = self.sqlite_man.load_all()
        print(f"SQLite link database file connected: {len(self.links)} microwave link's definitions loaded.")

        # \\\\\\ END APP LOGIC CONSTRUCTOR //////

    # influxDB's status changed GUI method
    def _db_status_changed(self, status: bool):
        if status:  # True == connected
            self.status_db_state.setText("Connected")
            self.status_db_icon_lbl.setPixmap(QPixmap('./gui/icons/check_green.png'))
        else:       # False == disconnected
            self.status_db_state.setText("Disconnected")
            self.status_db_icon_lbl.setPixmap(QPixmap('./gui/icons/cross_red.png'))

    # insert InfluxDB's status checker into threadpool and start it, called by timer
    def _pool_checker(self):
        influx_checker = influx.InfluxChecker(self.influx_signals)
        self.threadpool.start(influx_checker)

    # influxDB's status selection logic, called from signal
    def check_influx_status(self, influx_ping: bool):
        if influx_ping and self.influx_status == 0:
            self._db_status_changed(True)
            print("InfluxDB connection has been established.", flush=True)
            self.influx_status = 1
        elif not influx_ping and self.influx_status == 0:
            self._db_status_changed(False)
            print("InfluxDB connection is not available.", flush=True)
            self.influx_status = -1
            # TODO: show warning dialog
        elif not influx_ping and self.influx_status == 1:
            self._db_status_changed(False)
            print("InfluxDB connection has been lost.", flush=True)
            self.influx_status = -1
            # TODO: show warning dialog
        elif influx_ping and self.influx_status == -1:
            self._db_status_changed(True)
            print("InfluxDB connection has been reestablished.", flush=True)
            self.influx_status = 1

    # calculation button fired
    def calculation_start(self):
        # check if InfluxDB connection is available
        if self.influx_status != 1:
            print("[WARNING] Cannot start calculation, InfluxDB connection is not available.")
            self.statusBar().showMessage("Cannot start calculation, InfluxDB connection is not available.")
            return

        start = self.datetime_start.dateTime()
        end = self.datetime_stop.dateTime()
        step = self.spin_timestep.value()

        if start >= end:
            print("[WARNING] Bad input! Entered bigger (or same) start date than end date!")
            self.statusBar().showMessage("Bad input! Entered bigger (or same) start date than end date!")
            # TODO: all the checks
        else:
            self.result_id += 1
            # link channel selection flag: 0=none, 1=A, 2=B, 3=both
            # TODO: create link list dynamic load; temp list below
            selection = {50236: 3, 50288: 0, 50462: 3, 50483: 3, 50508: 3, 50626: 3, 50687: 3, 50693: 3, 50829: 3,
                         50923: 3, 50952: 3, 51426: 3, 51578: 3, 51588: 3, 51621: 3, 51647: 3, 51650: 3, 51656: 3,
                         51689: 3, 51692: 3, 51827: 3, 51864: 3, 51865: 3, 51866: 3, 51881: 3, 51950: 3, 51955: 3,
                         51958: 3, 51972: 3, 52037: 3, 52038: 3, 52039: 3, 52052: 3, 52062: 3, 52090: 3, 52098: 3,
                         52146: 3, 52154: 3, 52157: 3, 52161: 3, 52211: 3, 52215: 3, 52342: 3, 52367: 3, 52485: 3,
                         52486: 3, 52517: 3, 52539: 3, 52543: 3, 52549: 3, 52560: 3, 52562: 3, 52572: 3, 52617: 3,
                         52624: 3, 52626: 3, 52710: 3, 52736: 3, 52739: 3, 52741: 3, 52752: 3, 52771: 3, 52782: 3,
                         52799: 3, 52818: 3, 52834: 3, 52845: 3, 52846: 3, 52864: 3, 52886: 3, 52915: 3, 52935: 3,
                         52994: 3, 50845: 3, 50988: 3, 51018: 3, 51022: 3, 51200: 3, 51231: 3, 51274: 3, 51340: 3,
                         51499: 3, 51657: 3, 51680: 3, 51746: 3, 51794: 3, 51821: 3, 52024: 3, 52040: 3, 52189: 3,
                         52191: 3, 52308: 3, 52412: 3, 52526: 3, 52566: 3, 52708: 3, 52748: 3, 52901: 3, 51188: 3,
                         52346: 3}

            # create calculation instance
            calculation = calc.Calculation(self.calc_signals, self.result_id, self.links, selection, start, end, step)

            if self.results_name.text() == "":
                results_tab_name = "<no name>"
            else:
                results_tab_name = self.results_name.text()

            # create results widget instance
            self.results_tabs[self.result_id] = ResultsWidget(results_tab_name)

            self.results_name.clear()

            # RUN calculation on worker thread from threadpool
            self.threadpool.start(calculation)
            # calculation.run()  # TEMP: run directly on gui thread for debugging reasons

            self.statusBar().showMessage("Processing...")

    # show results from calculation, called from signal
    def show_results(self, meta_data: dict):
        # plot data in results tab
        self.results_tabs[meta_data["id"]].update_main_plot(meta_data["interpolator"],
                                                            meta_data["rain_grid"],
                                                            meta_data["cmls_rain_1h"])

        # insert results tab to tab list
        self.tabs.addTab(self.results_tabs[meta_data["id"]], self.results_icon,
                         f"Results: {self.results_tabs[meta_data['id']].tab_name}")

        self.statusBar().showMessage(f"Calculation \"{self.results_tabs[meta_data['id']].tab_name}\" is complete.")

    def calculation_error(self, meta_data: dict):
        self.statusBar().showMessage(f"Error occurred in calculation \"{self.results_tabs[meta_data['id']].tab_name}\"."
                                     f" See system log for more info.")

    # destructor
    def __del__(self):
        # return stdout to default state
        sys.stdout = sys.__stdout__
