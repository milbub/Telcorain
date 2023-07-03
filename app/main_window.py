import sys

from PyQt6 import uic, QtGui, QtCore
from PyQt6.QtCore import QTimer, QObject
from PyQt6.QtGui import QPixmap, QAction
from PyQt6.QtWidgets import QMainWindow, QLabel, QProgressBar, QHBoxLayout, QWidget, QTextEdit, QListWidget, \
    QDateTimeEdit, QPushButton, QSpinBox, QTabWidget, QLineEdit, QDoubleSpinBox, QRadioButton, QCheckBox, \
    QListWidgetItem, QTableWidget, QGridLayout, QMessageBox, QFileDialog, QApplication, QComboBox
from datetime import datetime, timedelta

from database.influx_manager import InfluxManager, InfluxChecker, InfluxSignals
from database.sql_manager import SqlManager, SqlChecker, SqlSignals
from writers.config_manager import ConfigManager
from writers.linksets_manager import LinksetsManager
from writers.log_manager import LogManager
from writers.realtime_writer import RealtimeWriter
from procedures.calculation import Calculation, CalcSignals

from app.form_dialog import FormDialog
from app.selection_dialog import SelectionDialog
from app.results_widget import ResultsWidget


# TODO: move Control Tab elements into separate widget. Currently, this class contains main logic + Control Tab widgets.

class MainWindow(QMainWindow):
    def __init__(self):
        super(MainWindow, self).__init__()

        # ////// GUI CONSTRUCTOR \\\\\\

        # set up window icon
        self.app_icon = QtGui.QIcon()
        self.app_icon.addFile('./app/gui/icons/app_16x16.png', QtCore.QSize(16, 16))
        self.app_icon.addFile('./app/gui/icons/app_32x32.png', QtCore.QSize(32, 32))
        self.app_icon.addFile('./app/gui/icons/app_96x96.png', QtCore.QSize(48, 48))
        self.setWindowIcon(self.app_icon)

        # load UI definition from Qt XML file
        uic.loadUi("./app/gui/MainWindow.ui", self)

        # set up statusbar - InfluxDB connection state
        self.status_db = QWidget()
        self.status_db_layout = QHBoxLayout()
        self.status_db_layout.setSpacing(0)
        self.status_db.setLayout(self.status_db_layout)
        self.status_db_label = QLabel()
        self.status_db_label.setText("InfluxDB:")
        self.status_db_layout.addWidget(self.status_db_label)
        self.status_db_icon_lbl = QLabel()
        self.status_db_icon_lbl.setPixmap(QPixmap('./app/gui/icons/cross_red.png'))
        self.status_db_layout.addWidget(self.status_db_icon_lbl)
        self.status_db_state = QLabel()
        self.status_db_state.setText("Disconnected")
        self.status_db_layout.addWidget(self.status_db_state)
        self.statusBar().addPermanentWidget(self.status_db)

        # set up statusbar - MariaDB connection state
        self.status_sql = QWidget()
        self.status_sql_layout = QHBoxLayout()
        self.status_sql_layout.setSpacing(0)
        self.status_sql.setLayout(self.status_sql_layout)
        self.status_sql_label = QLabel()
        self.status_sql_label.setText("MariaDB:")
        self.status_sql_layout.addWidget(self.status_sql_label)
        self.status_sql_icon_lbl = QLabel()
        self.status_sql_icon_lbl.setPixmap(QPixmap('./app/gui/icons/cross_red.png'))
        self.status_sql_layout.addWidget(self.status_sql_icon_lbl)
        self.status_sql_state = QLabel()
        self.status_sql_state.setText("Disconnected")
        self.status_sql_layout.addWidget(self.status_sql_state)
        self.statusBar().addPermanentWidget(self.status_sql)

        # set up statusbar - calculation progress bar
        self.status_prg = QWidget()
        self.status_prg_layout = QHBoxLayout()
        self.status_prg.setLayout(self.status_prg_layout)
        self.status_prg.setFixedWidth(220)
        self.status_prg_bar = QProgressBar()
        self.status_prg_bar.setFixedWidth(200)
        self.status_prg_bar.setTextVisible(False)
        self.status_prg_bar.setMinimum(0)
        self.status_prg_bar.setMaximum(99)
        self.status_prg_layout.addWidget(self.status_prg_bar)
        self.statusBar().addPermanentWidget(self.status_prg)

        # set spacing for status bar
        self.statusBar().setContentsMargins(14, 0, 14, 0)

        # lookup for used actions and define them
        self.exit_action: QObject = self.findChild(QAction, "actionExit")
        self.exit_action.triggered.connect(QApplication.quit)

        # lookup for used widgets and define them
        self.text_log: QObject = self.findChild(QTextEdit, "textLog")
        self.lists: QObject = self.findChild(QListWidget, "listLists")
        self.selection_table: QObject = self.findChild(QTableWidget, "tableSelection")
        self.butt_new_set: QObject = self.findChild(QPushButton, "buttLstNew")
        self.butt_edit_set: QObject = self.findChild(QPushButton, "buttLstEdit")
        self.butt_copy_set: QObject = self.findChild(QPushButton, "buttLstCopy")
        self.butt_del_set: QObject = self.findChild(QPushButton, "buttLstDel")
        self.datetime_start: QObject = self.findChild(QDateTimeEdit, "dateTimeStart")
        self.datetime_stop: QObject = self.findChild(QDateTimeEdit, "dateTimeStop")
        self.spin_timestep: QObject = self.findChild(QSpinBox, "spinTimestep")
        self.butt_start: QObject = self.findChild(QPushButton, "buttStart")
        self.butt_abort: QObject = self.findChild(QPushButton, "buttAbort")
        self.tabs: QObject = self.findChild(QTabWidget, "tabWidget")
        self.results_name: QObject = self.findChild(QLineEdit, "resultsNameEdit")
        self.spin_roll_window: QObject = self.findChild(QDoubleSpinBox, "spinRollWindow")
        self.spin_wet_dry_sd: QObject = self.findChild(QDoubleSpinBox, "spinWetDrySD")
        self.spin_baseline_samples: QObject = self.findChild(QSpinBox, "spinBaselineSamples")
        self.spin_output_step: QObject = self.findChild(QSpinBox, "spinOutputStep")
        self.spin_interpol_res: QObject = self.findChild(QDoubleSpinBox, "spinInterResolution")
        self.spin_idw_power: QObject = self.findChild(QSpinBox, "spinIdwPower")
        self.spin_idw_near: QObject = self.findChild(QSpinBox, "spinIdwNear")
        self.spin_idw_dist: QObject = self.findChild(QDoubleSpinBox, "spinIdwDist")
        self.radio_output_total: QObject = self.findChild(QRadioButton, "radioOutputTotal")
        self.box_only_overall: QObject = self.findChild(QCheckBox, "checkOnlyOverall")
        self.path_box: QObject = self.findChild(QLineEdit, "editPath")
        self.butt_choose_path: QObject = self.findChild(QPushButton, "buttChoosePath")
        self.pdf_box: QObject = self.findChild(QCheckBox, "checkFilePDF")
        self.png_box: QObject = self.findChild(QCheckBox, "checkFilePNG")
        self.check_dummy: QObject = self.findChild(QCheckBox, "checkDummy")
        self.spin_waa_schleiss_val: QObject = self.findChild(QDoubleSpinBox, "spinSchleissWaa")
        self.spin_waa_schleiss_tau: QObject = self.findChild(QDoubleSpinBox, "spinSchleissTau")
        self.radio_historic: QObject = self.findChild(QRadioButton, "radioHistoric")
        self.radio_realtime: QObject = self.findChild(QRadioButton, "radioRealtime")
        self.combo_realtime: QObject = self.findChild(QComboBox, "comboRealtime")
        self.label_realtime: QObject = self.findChild(QLabel, "labelRealtime")
        self.correlation_spin: QObject = self.findChild(QDoubleSpinBox, "correlationSpin")
        self.correlation_filter_box: QObject = self.findChild(QCheckBox, "filterCMLsBox")
        self.compensation_box: QObject = self.findChild(QCheckBox, "compensationBox")
        self.write_output_box: QObject = self.findChild(QCheckBox, "writeOutputBox")
        self.window_pointer_combo: QObject = self.findChild(QComboBox, "windowPointerCombo")

        # declare dictionary for created tabs with calculation results
        # <key: int = result ID, value: ResultsWidget>
        self.results_tabs = {}

        # results tabs ID counter
        self.result_id = 0

        # icon for results tabs
        self.results_icon = QtGui.QIcon()
        self.results_icon.addFile('./app/gui/icons/explore.png', QtCore.QSize(16, 16))

        # connect buttons
        self.butt_start.clicked.connect(self.calculation_fired)
        self.butt_abort.clicked.connect(self.calculation_cancel_fired)
        self.butt_new_set.clicked.connect(self.new_linkset_fired)
        self.butt_edit_set.clicked.connect(self.edit_linkset_fired)
        self.butt_copy_set.clicked.connect(self.copy_linkset_fired)
        self.butt_del_set.clicked.connect(self.delete_linkset_fired)
        self.butt_choose_path.clicked.connect(self.choose_path_fired)

        # connect other signals
        self.spin_timestep.valueChanged.connect(lambda a: self.spin_roll_window.setValue(a * 36 / 60))
        self.radio_realtime.clicked.connect(lambda a: self.box_only_overall.setChecked(False))
        self.radio_realtime.clicked.connect(lambda a: self.window_pointer_combo.setCurrentIndex(1))
        self.radio_historic.clicked.connect(lambda a: self.write_output_box.setChecked(False))

        # style out link table
        self.selection_table.setColumnWidth(0, 40)
        self.selection_table.setColumnWidth(1, 42)
        self.selection_table.setColumnWidth(2, 42)
        self.selection_table.setColumnWidth(3, 75)
        self.selection_table.setColumnWidth(4, 71)
        self.selection_table.setColumnWidth(5, 75)
        self.selection_table.setColumnWidth(6, 148)

        # style out other things
        self.combo_realtime.setHidden(True)
        self.label_realtime.setHidden(True)

        # show window
        self.show()

        # ////// APP LOGIC CONSTRUCTOR \\\\\\

        # init core managers
        self.config_man = ConfigManager()
        self.log_man = LogManager(self.text_log)
        self.sql_man = SqlManager(self.config_man)
        self.influx_man = InfluxManager(self.config_man)

        # redirect stdout to log handler
        sys.stdout = self.log_man
        print("Telcorain is starting...", flush=True)

        # init threadpool
        self.threadpool = QtCore.QThreadPool()

        # init app logic signaling
        self.influx_signals = InfluxSignals()
        self.calc_signals = CalcSignals()
        self.sql_signals = SqlSignals()

        # DBs status signals
        self.influx_signals.ping_signal.connect(self.check_influx_status)
        self.sql_signals.ping_signal.connect(self.check_sql_status)

        # rainfall calculation signals
        self.calc_signals.overall_done_signal.connect(self.show_overall_results)
        self.calc_signals.plots_done_signal.connect(self.show_animation_results)
        self.calc_signals.error_signal.connect(self.calculation_error)
        self.calc_signals.progress_signal.connect(self.progress_update)

        # init DBs connection and status checkers
        self.influx_status: int = 0  # 0 = unknown, 1 = ok, -1 = not available
        self.sql_status: int = 0  # 0 = unknown, 1 = ok, -1 = not available

        self.influx_checker = InfluxChecker(self.config_man, self.influx_signals)
        self.influx_checker.setAutoDelete(False)
        self._pool_influx_checker()   # first Influx connection check
        self.influx_timer = QTimer()  # create timer for next checks
        self.influx_timer.timeout.connect(self._pool_influx_checker)
        # TODO: load influx timeout from config and add some time
        self.influx_timer.start(5000)

        self.sql_checker = SqlChecker(self.config_man, self.sql_signals)
        self.sql_checker.setAutoDelete(False)
        self._pool_sql_checker()   # first MariaDB connection check
        self.sql_timer = QTimer()  # create timer for next checks
        self.sql_timer.timeout.connect(self._pool_sql_checker)
        # TODO: load MariaDB timeout from config and add some time
        self.sql_timer.start(5000)

        # load CML definitions from SQL database
        self.links = self.sql_man.load_metadata()
        print(f"{len(self.links)} microwave link's definitions loaded from MariaDB.")

        # init link sets
        self.sets_man = LinksetsManager(self.links)
        self.current_selection = {}   # link channel selection flag: 0=none, 1=A, 2=B, 3=both -> dict: <link_id>: flag
        self.lists.currentTextChanged.connect(self._linkset_selected)

        # add default value to list of link's list (ALL = list of all links)
        default_option = QListWidgetItem('<ALL>')
        self.lists.addItem(default_option)
        self.lists.setCurrentItem(default_option)

        # fill with other sets
        self._fill_linksets()

        # output default path, TODO: load from options
        self.path = './outputs'
        self.path_box.setText(self.path + '/<time>')

        # prepare realtime calculation slot and timer
        self.running_realtime = None
        self.realtime_timer = QTimer()
        self.realtime_timer.setSingleShot(True)
        self.realtime_timer.timeout.connect(self._pool_realtime_run)
        self.realtime_last_run: datetime = datetime.min
        self.is_realtime_showed: bool = False

    # influxDB's status selection logic, called from signal
    def check_influx_status(self, influx_ping: bool):
        if influx_ping and self.influx_status == 0:
            self._influx_status_changed(True)
            print("InfluxDB connection has been established.", flush=True)
            self.influx_status = 1
        elif not influx_ping and self.influx_status == 0:
            self._influx_status_changed(False)
            print("InfluxDB connection is not available.", flush=True)
            self.influx_status = -1
        elif not influx_ping and self.influx_status == 1:
            self._influx_status_changed(False)
            print("InfluxDB connection has been lost.", flush=True)
            self.influx_status = -1
        elif influx_ping and self.influx_status == -1:
            self._influx_status_changed(True)
            print("InfluxDB connection has been reestablished.", flush=True)
            self.influx_status = 1

    # MariaDB's status selection logic, called from signal
    def check_sql_status(self, sql_ping: bool):
        if sql_ping and self.sql_status == 0:
            self._sql_status_changed(True)
            print("MariaDB connection has been established.", flush=True)
            self.sql_status = 1
        elif not sql_ping and self.sql_status == 0:
            self._sql_status_changed(False)
            print("MariaDB connection is not available.", flush=True)
            self.sql_status = -1
        elif not sql_ping and self.sql_status == 1:
            self._sql_status_changed(False)
            print("MariaDB connection has been lost.", flush=True)
            self.sql_status = -1
        elif sql_ping and self.sql_status == -1:
            self._sql_status_changed(True)
            print("MariaDB connection has been reestablished.", flush=True)
            self.sql_status = 1

    # show overall results from calculation, called from signal
    def show_overall_results(self, meta_data: dict):
        # plot data in results tab
        self.results_tabs[meta_data["id"]].render_overall_fig(meta_data["x_grid"],
                                                              meta_data["y_grid"],
                                                              meta_data["rain_grid"],
                                                              meta_data["link_data"])

        if not self.is_realtime_showed:
            # insert results tab to tab list
            self.tabs.addTab(self.results_tabs[meta_data["id"]], self.results_icon,
                             f"Results: {self.results_tabs[meta_data['id']].tab_name}")
            if self.running_realtime is not None:
                self.is_realtime_showed = True

        if meta_data["is_it_all"]:
            self.statusBar().showMessage(f"Calculation \"{self.results_tabs[meta_data['id']].tab_name}\" is complete.")

            # restore buttons
            self.butt_abort.setEnabled(False)
            self.butt_start.setEnabled(True)
        else:
            self.statusBar().showMessage(f"Overall plot in calculation \"{self.results_tabs[meta_data['id']].tab_name}"
                                         f"\" is complete. Animation figures are now interpolated...")
            self.results_tabs[meta_data["id"]].change_no_anim_notification(still_interpolating=True)

        # return progress bar to default state
        self.status_prg_bar.setValue(0)

    # show animation results from calculation, called from signal
    def show_animation_results(self, meta_data: dict):
        # show animation data in results tab
        self.results_tabs[meta_data["id"]].render_first_animation_fig(meta_data["x_grid"],
                                                                      meta_data["y_grid"],
                                                                      meta_data["rain_grids"],
                                                                      meta_data["link_data"])

        # return progress bar to default state
        self.status_prg_bar.setValue(0)

        if self.running_realtime is not None:
            timediff = datetime.now() - self.realtime_last_run
            interval = self.results_tabs[meta_data['id']].output_step * 60 + 10

            if timediff.total_seconds() < interval:
                self.butt_abort.setEnabled(True)
                msg = str(f"Realtime iteration #{self.running_realtime.realtime_runs} of calculation "
                          f"\"{self.results_tabs[meta_data['id']].tab_name}\" is complete. "
                          f"Next iteration starts in {int(interval - timediff.total_seconds())} seconds.")
                self.realtime_timer.start(int(interval - timediff.total_seconds()) * 1000)
            else:
                msg = str(f"Processing realtime calculation iteration #{self.running_realtime.realtime_runs}...")
                self._pool_realtime_run()

        else:
            self.butt_start.setEnabled(True)
            msg = str(f"Calculation \"{self.results_tabs[meta_data['id']].tab_name}\" is complete.")

        del meta_data
        self.statusBar().showMessage(msg)

    # show info about error in calculation, called from signal
    def calculation_error(self, meta_data: dict):
        self.statusBar().showMessage(f"Error occurred in calculation \"{self.results_tabs[meta_data['id']].tab_name}\"."
                                     f" See system log for more info.")

        # return progress bar to default state
        self.status_prg_bar.setValue(0)

        # restore buttons
        self.butt_start.setEnabled(True)

    # update progress bar with calculation status, called from signal
    def progress_update(self, meta_data: dict):
        self.status_prg_bar.setValue(meta_data['prg_val'])

    # calculation button fired
    def calculation_fired(self):
        # check if InfluxDB connection is available
        if self.influx_status != 1:
            msg = "Cannot start calculation, InfluxDB connection is not available."
            print(f"[WARNING] {msg}")
            self.statusBar().showMessage(msg)
            return

        # get parameters from Qt widgets
        start = self.datetime_start.dateTime()
        end = self.datetime_stop.dateTime()
        step = self.spin_timestep.value()
        time_diff = start.msecsTo(end)
        rolling_hours = self.spin_roll_window.value()
        rolling_values = int((rolling_hours * 60) / step)
        wet_dry_deviation = self.spin_wet_dry_sd.value()
        baseline_samples = self.spin_baseline_samples.value()
        interpol_res = self.spin_interpol_res.value()
        idw_power = self.spin_idw_power.value()
        idw_near = self.spin_idw_near.value()
        idw_dist = self.spin_idw_dist.value()
        output_step = self.spin_output_step.value()
        is_only_overall = self.box_only_overall.isChecked()
        is_output_total = self.radio_output_total.isChecked()
        is_pdf = self.pdf_box.isChecked()
        is_png = self.png_box.isChecked()
        is_dummy = self.check_dummy.isChecked()
        waa_schleiss_val = self.spin_waa_schleiss_val.value()
        waa_schleiss_tau = self.spin_waa_schleiss_tau.value()
        close_func = self.close_tab_result
        is_correlation = self.compensation_box.isChecked()
        spin_correlation = self.correlation_spin.value()
        combo_realtime = self.combo_realtime.currentText()
        is_realtime = self.radio_realtime.isChecked()
        is_remove = self.correlation_filter_box.isChecked()
        is_output_write = self.write_output_box.isChecked()
        is_window_centered = True if self.window_pointer_combo.currentIndex() == 0 else False
        retention = int(self.config_man.read_option('realtime', 'retention'))
        X_MIN = float(self.config_man.read_option('rendering', 'X_MIN'))
        X_MAX = float(self.config_man.read_option('rendering', 'X_MAX'))
        Y_MIN = float(self.config_man.read_option('rendering', 'Y_MIN'))
        Y_MAX = float(self.config_man.read_option('rendering', 'Y_MAX'))

        # for writing output data back into DB, we need working MariaDB connection
        if is_output_write and self.sql_status != 1:
            msg = "Cannot start realtime calculation, MariaDB connection is not available."
            print(f"[WARNING] {msg}")
            self.statusBar().showMessage(msg)
            return

        # INPUT CHECKS:
        if time_diff < 0:   # if timediff is less than 1 hour (in msecs)
            msg = "Bad input! Entered bigger (or same) start date than end date!"
            print(f"[WARNING] {msg}")
        elif time_diff < 3600000:
            msg = "Bad input! Time difference between start and end times must be at least 1 hour."
            print(f"[WARNING] {msg}")
        elif (time_diff / (step * 60000)) < 12:
            # TODO: load magic constant 12 from options
            msg = "Bad input! Data resolution must be at least 12 times lower than input time interval length."
            print(f"[WARNING] {msg}")
        elif rolling_values < 6:
            # TODO: load magic constant 6 from options
            msg = f"Rolling time window length must be, for these times, at least {(step * 6) / 60} hours."
            print(f"[WARNING] {msg}")
        elif (rolling_hours * 3600000) > time_diff:
            msg = f"Rolling time window length cannot be longer than set time interval."
            print(f"[WARNING] {msg}")
        elif output_step < step:
            msg = f"Output frame interval cannot be shorter than initial data resolution."
            print(f"[WARNING] {msg}")
        elif step > 59:
            msg = f"Input time interval cannot be longer than 59 minutes."
            print(f"[WARNING] {msg}")
        else:
            self.result_id += 1

            # create calculation instance
            calculation = Calculation(self.influx_man, self.calc_signals, self.result_id, self.links,
                                      self.current_selection, start, end, step, rolling_values, output_step,
                                      is_only_overall, is_output_total, wet_dry_deviation, baseline_samples,
                                      interpol_res, idw_power, idw_near, idw_dist, waa_schleiss_val, waa_schleiss_tau,
                                      is_correlation, spin_correlation, combo_realtime, not is_realtime, is_remove,
                                      is_window_centered)

            if self.results_name.text() == "":
                results_tab_name = "<no name>"
            else:
                results_tab_name = self.results_name.text()

            # pass some calc params into the results in dict
            params = {
                'roll': rolling_hours,
                'sd': wet_dry_deviation,
                'base_smp': baseline_samples,
                'resolution': interpol_res,
                'pow': idw_power,
                'near': idw_near,
                'dist': idw_dist,
                'schleiss_m': waa_schleiss_val,
                'schleiss_t': waa_schleiss_tau,
            }

            # create results widget instance
            if is_output_write:
                start_time = datetime.utcnow()
                output_delta = timedelta(minutes=output_step)
                since_time = start_time - output_delta

                realtime_w = RealtimeWriter(self.sql_man, self.influx_man, False, since_time)
                self.results_tabs[self.result_id] = ResultsWidget(results_tab_name, self.result_id, start, end,
                                                                  output_step, is_output_total, self.path, is_pdf,
                                                                  is_png, close_func, is_only_overall, is_dummy, params,
                                                                  realtime_writer=realtime_w)
            else:
                self.results_tabs[self.result_id] = ResultsWidget(results_tab_name, self.result_id, start, end,
                                                                  output_step, is_output_total, self.path, is_pdf,
                                                                  is_png, close_func, is_only_overall, is_dummy, params,
                                                                  realtime_writer=None)

            self.results_name.clear()
            self.butt_start.setEnabled(False)

            if is_realtime:
                calculation.setAutoDelete(False)
                self.running_realtime = calculation

                if is_output_write:
                    params = self.sql_man.get_last_realtime()
                    print("Realtime outputs writing activated!")
                    if len(params) != 0:
                        print(f"Last written realtime calculation started at "
                              f"{params['start_time'].strftime('%Y-%m-%d %H:%M:%S')} and ran with parameters: "
                              f"retention: {(params['retention']/60):.0f} h, step: {(params['timestep']/60):.0f} min, "
                              f"grid resolution: {(params['resolution']):.8f} °.")
                    print(f"Current realtime parameters are: retention: {retention} h, step: "
                          f"{output_step} min, grid resolution: {interpol_res:.8f} °.")
                    self.sql_man.insert_realtime(retention * 60, output_step * 60, interpol_res,
                                                 X_MIN, X_MAX, Y_MIN, Y_MAX)

                self._pool_realtime_run()
                msg = "Processing realtime calculation iteration..."
            else:
                # RUN calculation on worker thread from threadpool
                self.threadpool.start(calculation)
                # calculation.run()  # TEMP: run directly on gui thread for debugging reasons
                msg = "Processing..."

        self.statusBar().showMessage(msg)

    def calculation_cancel_fired(self):
        if self.running_realtime is not None:
            self.realtime_timer.stop()
            self.statusBar().showMessage(f"Realtime calculation has been interrupted after "
                                         f"{self.running_realtime.realtime_runs} runs.")
            self.running_realtime = None
            self.is_realtime_showed = False
            self.butt_start.setEnabled(True)
            self.butt_abort.setEnabled(False)

    def new_linkset_fired(self):
        dialog = FormDialog(self, "Link Set Creation", "Please, enter a name of the new link set:")

        if dialog.exec():
            name = dialog.answer_box.text()

            if name == '':
                self._show_empty_entry_warning()
                return

            self.sets_man.create_set(name)
            new_item = QListWidgetItem(name)
            self.lists.addItem(new_item)
            self.lists.setCurrentItem(new_item)

            self.statusBar().showMessage(f'Link set "{name}" was created.')

    def edit_linkset_fired(self):
        sel_name = self.lists.currentItem().text()
        dialog = SelectionDialog(self, self.current_selection, self.links)

        if dialog.exec():
            self.current_selection = dialog.selection

            # overwrite values in set file
            for link_id in self.current_selection:
                # set file has reverse logic -> in DEFAULT section (link set ALL), all links are there with
                # BOTH CHANNELS (= flag 3) defined, while in subsections (link sets), only links with different status
                # (= flags 1 and 2) are defined in them, and the rest (= flag 3) is INHERITED from DEFAULT section
                # ==> THEREFORE, links with flag 3 are deleted from edited link set, and the rest is modified

                if self.current_selection[link_id] == 3:
                    self.sets_man.delete_link(sel_name, link_id)
                else:
                    self.sets_man.modify_link(sel_name, link_id, self.current_selection[link_id])

            self.sets_man.save()
            self._linkset_selected(sel_name)

    def copy_linkset_fired(self):
        sel_name = self.lists.currentItem().text()
        dialog = FormDialog(self, "Link Set Copy", "Please, enter a name of the new link set copy:")

        if dialog.exec():
            new_name = dialog.answer_box.text()

            if new_name == '':
                self._show_empty_entry_warning()
                return

            self.sets_man.copy_set(sel_name, new_name)
            new_item = QListWidgetItem(new_name)
            self.lists.addItem(new_item)
            self.lists.setCurrentItem(new_item)

            self.statusBar().showMessage(f'New copy "{new_name}" of link set "{sel_name}" was created.')

    def delete_linkset_fired(self):
        selected = self.lists.currentItem()
        sel_name = selected.text()

        dialog = QMessageBox(self)
        dialog.setWindowTitle("Are you sure?")
        dialog.setText(f'You want to delete link set "{sel_name}". Are you sure?')
        dialog.setStandardButtons(QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel)
        dialog.setIcon(QMessageBox.Icon.Question)
        answer = dialog.exec()

        if answer == QMessageBox.StandardButton.Yes:
            self.sets_man.delete_set(sel_name)
            self.lists.takeItem(self.lists.row(selected))

            self.statusBar().showMessage(f'Link set "{sel_name}" was deleted.')

    def choose_path_fired(self):
        self.path = QFileDialog.getExistingDirectory(self, 'Select folder for outputs', self.path)
        if self.path == '':
            self.path = './outputs'
        self.path_box.setText(self.path + '/<time>')

    def close_tab_result(self, result_id: int):
        self.tabs.removeTab(self.tabs.currentIndex())
        self.results_tabs.pop(result_id)
        self.tabs.setCurrentIndex(0)

    def _show_empty_entry_warning(self):
        info = QMessageBox(self)
        info.setWindowTitle("Attention!")
        info.setText("Entered name cannot be empty.")
        info.setStandardButtons(QMessageBox.StandardButton.Ok)
        info.setIcon(QMessageBox.Icon.Warning)
        info.exec()

    # influxDB's status changed GUI method
    def _influx_status_changed(self, status: bool):
        if status:  # True == connected
            self.status_db_state.setText("Connected")
            self.status_db_icon_lbl.setPixmap(QPixmap('./app/gui/icons/check_green.png'))
        else:       # False == disconnected
            self.status_db_state.setText("Disconnected")
            self.status_db_icon_lbl.setPixmap(QPixmap('./app/gui/icons/cross_red.png'))

    # MariaDB's status changed GUI method
    def _sql_status_changed(self, status: bool):
        if status:  # True == connected
            self.status_sql_state.setText("Connected")
            self.status_sql_icon_lbl.setPixmap(QPixmap('./app/gui/icons/check_green.png'))
        else:       # False == disconnected
            self.status_sql_state.setText("Disconnected")
            self.status_sql_icon_lbl.setPixmap(QPixmap('./app/gui/icons/cross_red.png'))

    # insert InfluxDB's status checker into threadpool and start it, called by timer
    def _pool_influx_checker(self):
        self.threadpool.start(self.influx_checker)

    # insert MariaDB's status checker into threadpool and start it, called by timer
    def _pool_sql_checker(self):
        self.threadpool.start(self.sql_checker)

    def _pool_realtime_run(self):
        self.butt_abort.setEnabled(False)
        self.realtime_last_run = datetime.now()
        self.threadpool.start(self.running_realtime)

    def _linkset_selected(self, selection: str):
        if selection == '<ALL>':
            sel = self.sets_man.linksets['DEFAULT']
            self.butt_edit_set.setEnabled(False)
            self.butt_copy_set.setEnabled(False)
            self.butt_del_set.setEnabled(False)
        else:
            sel = self.sets_man.linksets[selection]
            self.butt_edit_set.setEnabled(True)
            self.butt_copy_set.setEnabled(True)
            self.butt_del_set.setEnabled(True)

        active_count = 0
        for link_id in sel:
            self.current_selection[int(link_id)] = int(sel[link_id])

            if sel[link_id] == str(0):
                continue

            active_count += 1

        self.selection_table.clearContents()
        self.selection_table.setRowCount(active_count)

        # columns: 0 = ID, 1 = channel 1, 2 = channel 2, 3 = technology, 4 = band, 5 = length, 6 = name
        row = 0
        for link_id in self.current_selection:
            if self.current_selection[link_id] == 0:
                continue

            id_label = QLabel(str(link_id))
            id_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            tech_label = QLabel(self.links[link_id].tech)
            tech_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            band_label = QLabel("{:.0f}".format(self.links[link_id].freq_a / 1000))
            band_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            length_label = QLabel("{:.2f}".format(self.links[link_id].distance))
            length_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            name_label = QLabel(self.links[link_id].name)

            channel_1 = QCheckBox()
            channel_2 = QCheckBox()

            if self.current_selection[link_id] == 1:
                channel_1.setChecked(True)
                channel_2.setChecked(False)
            elif self.current_selection[link_id] == 2:
                channel_1.setChecked(False)
                channel_2.setChecked(True)
            elif self.current_selection[link_id] == 3:
                channel_1.setChecked(True)
                channel_2.setChecked(True)

            # Qt TableWidget formatting weirdness:

            channel_1.setAttribute(QtCore.Qt.WidgetAttribute.WA_TransparentForMouseEvents)
            channel_1.setFocusPolicy(QtCore.Qt.FocusPolicy.NoFocus)
            channel_2.setAttribute(QtCore.Qt.WidgetAttribute.WA_TransparentForMouseEvents)
            channel_2.setFocusPolicy(QtCore.Qt.FocusPolicy.NoFocus)

            channel_1_box = QGridLayout()
            channel_1_box.addWidget(channel_1, 0, 0, alignment=QtCore.Qt.AlignmentFlag.AlignCenter)
            channel_1_box.setContentsMargins(0, 0, 0, 0)
            channel_1_box_box = QWidget()
            channel_1_box_box.setLayout(channel_1_box)

            channel_2_box = QGridLayout()
            channel_2_box.addWidget(channel_2, 0, 0, alignment=QtCore.Qt.AlignmentFlag.AlignCenter)
            channel_2_box.setContentsMargins(0, 0, 0, 0)
            channel_2_box_box = QWidget()
            channel_2_box_box.setLayout(channel_2_box)

            self.selection_table.setCellWidget(row, 0, id_label)
            self.selection_table.setCellWidget(row, 1, channel_1_box_box)
            self.selection_table.setCellWidget(row, 2, channel_2_box_box)
            self.selection_table.setCellWidget(row, 3, tech_label)
            self.selection_table.setCellWidget(row, 4, band_label)
            self.selection_table.setCellWidget(row, 5, length_label)
            self.selection_table.setCellWidget(row, 6, name_label)

            row += 1

    def _fill_linksets(self):
        for link_set in self.sets_man.sections:
            self.lists.addItem(link_set)

    # destructor
    def __del__(self):
        # return stdout to default state
        sys.stdout = sys.__stdout__
