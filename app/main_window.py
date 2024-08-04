from datetime import datetime, timedelta
from typing import cast

from PyQt6 import uic, QtGui, QtCore
from PyQt6.QtCore import QTimer, QObject
from PyQt6.QtGui import QPixmap, QAction
from PyQt6.QtWidgets import QMainWindow, QLabel, QProgressBar, QHBoxLayout, QWidget, QTextEdit, QListWidget, \
    QDateTimeEdit, QPushButton, QSpinBox, QTabWidget, QLineEdit, QDoubleSpinBox, QRadioButton, QCheckBox, \
    QListWidgetItem, QTableWidget, QMessageBox, QFileDialog, QApplication, QComboBox

from lib.pycomlink.pycomlink.processing.wet_dry.cnn import CNN_OUTPUT_LEFT_NANS_LENGTH

from database.influx_manager import influx_man, InfluxChecker, InfluxSignals
from database.sql_manager import sql_man, SqlChecker, SqlSignals
from handlers import config_handler
from handlers.linksets_handler import LinksetsHandler
from handlers.logging_handler import InitLogHandler, logger, setup_qt_logging
from handlers.realtime_writer import RealtimeWriter, purge_raw_outputs
from procedures.calculation import Calculation
from procedures.calculation_signals import CalcSignals

from app.form_dialog import FormDialog
from app.selection_dialog import SelectionDialog
from app.results_widget import ResultsWidget
from app.utils import LinksTableFactory

# TODO: move Control Tab elements into separate widget. Currently, this class contains main logic + Control Tab widgets.


class MainWindow(QMainWindow):
    def __init__(self, init_logger: InitLogHandler):
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
        self.text_log: QTextEdit = cast(QTextEdit, self.findChild(QTextEdit, "textLog"))
        self.lists: QObject = self.findChild(QListWidget, "listLists")
        self.selection_table: QTableWidget = cast(QTableWidget, self.findChild(QTableWidget, "tableSelection"))
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
        self.outputs_path_box: QObject = self.findChild(QLineEdit, "editPath")
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
        self.filter_box: QObject = self.findChild(QCheckBox, "boxTemperatureFilter")
        self.correlation_spin: QObject = self.findChild(QDoubleSpinBox, "correlationSpin")
        self.correlation_filter_box: QObject = self.findChild(QRadioButton, "radioRemove")
        self.compensation_box: QObject = self.findChild(QRadioButton, "radioCompensation")
        self.write_output_box: QObject = self.findChild(QCheckBox, "writeOutputBox")
        self.write_history_box: QObject = self.findChild(QCheckBox, "writeHistoryBox")
        self.skip_influx_write_box: QObject = self.findChild(QCheckBox, "boxSkipInfluxWrite")
        self.force_write_box: QObject = self.findChild(QCheckBox, "boxForce")
        self.window_pointer_combo: QObject = self.findChild(QComboBox, "windowPointerCombo")
        self.radio_cnn: QObject = self.findChild(QRadioButton, "radioCNN")
        self.action_external_filter: QObject = self.findChild(QAction, "actionExternalFilterLayer")

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
        self.radio_realtime.clicked.connect(lambda a: self.box_only_overall.setChecked(False))
        self.radio_realtime.clicked.connect(lambda a: self.window_pointer_combo.setCurrentIndex(1))
        self.radio_historic.clicked.connect(lambda a: self.write_output_box.setChecked(False))

        # init link table factory
        self.link_table_factory = LinksTableFactory(self.selection_table)

        # style out other things
        self.combo_realtime.setHidden(True)
        self.label_realtime.setHidden(True)
        self.force_write_box.setHidden(True)
        self.skip_influx_write_box.setHidden(True)

        # show window
        self.show()

        # ////// APP LOGIC CONSTRUCTOR \\\\\\
        # init Qt logger
        self.qt_logger = setup_qt_logging(self.text_log, init_logger, "DEBUG")

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

        self.influx_checker = InfluxChecker(self.influx_signals)
        self.influx_checker.setAutoDelete(False)
        self._pool_influx_checker()   # first Influx connection check
        self.influx_timer = QTimer()  # create timer for next checks
        self.influx_timer.timeout.connect(self._pool_influx_checker)
        # TODO: load influx timeout from config and add some time
        self.influx_timer.start(5000)

        self.sql_checker = SqlChecker(self.sql_signals)
        self.sql_checker.setAutoDelete(False)
        self._pool_sql_checker()   # first MariaDB connection check
        self.sql_timer = QTimer()  # create timer for next checks
        self.sql_timer.timeout.connect(self._pool_sql_checker)
        # TODO: load MariaDB timeout from config and add some time
        self.sql_timer.start(5000)

        # load CML definitions from SQL database
        self.links = sql_man.load_metadata()
        logger.info("%d microwave link's definitions loaded from MariaDB.", len(self.links))

        # init link sets
        self.sets_man = LinksetsHandler(self.links)
        self.current_selection = {}   # link channel selection flag: 0=none, 1=A, 2=B, 3=both -> dict: <link_id>: flag
        self.lists.currentTextChanged.connect(self._linkset_selected)

        # add default value to list of link's list (ALL = list of all links)
        default_option = QListWidgetItem('<ALL>')
        self.lists.addItem(default_option)
        self.lists.setCurrentItem(default_option)

        # fill with other sets
        self._fill_linksets()

        # output default path
        self.outputs_path = config_handler.read_option("directories", "outputs")
        self.outputs_path_box.setText(self.outputs_path + '/<time>')

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
            logger.info("InfluxDB connection has been established.")
            self.influx_status = 1
        elif not influx_ping and self.influx_status == 0:
            self._influx_status_changed(False)
            logger.warning("InfluxDB connection is not available.")
            self.influx_status = -1
        elif not influx_ping and self.influx_status == 1:
            self._influx_status_changed(False)
            logger.warning("InfluxDB connection has been lost.")
            self.influx_status = -1
        elif influx_ping and self.influx_status == -1:
            self._influx_status_changed(True)
            logger.info("InfluxDB connection has been reestablished.")
            self.influx_status = 1

    # MariaDB's status selection logic, called from signal
    def check_sql_status(self, sql_ping: bool):
        if sql_ping and self.sql_status == 0:
            self._sql_status_changed(True)
            logger.info("MariaDB connection has been established.")
            self.sql_status = 1
        elif not sql_ping and self.sql_status == 0:
            self._sql_status_changed(False)
            logger.warning("MariaDB connection is not available.")
            self.sql_status = -1
        elif not sql_ping and self.sql_status == 1:
            self._sql_status_changed(False)
            logger.warning("MariaDB connection has been lost.")
            self.sql_status = -1
        elif sql_ping and self.sql_status == -1:
            self._sql_status_changed(True)
            logger.info("MariaDB connection has been reestablished.")
            self.sql_status = 1

    # show overall results from calculation, called from signal
    def show_overall_results(self, sig_calc_dict: dict):
        # get ID results tab
        rs_id = sig_calc_dict["id"]

        # plot data in results tab
        self.results_tabs[rs_id].render_overall_fig(sig_calc_dict["start"], sig_calc_dict["end"],
                                                    sig_calc_dict["x_grid"], sig_calc_dict["y_grid"],
                                                    sig_calc_dict["rain_grid"], sig_calc_dict["calc_data"])

        if not self.is_realtime_showed:
            # insert results tab to tab list
            self.tabs.addTab(self.results_tabs[rs_id], self.results_icon,
                             f"Results: {self.results_tabs[rs_id].tab_name}")
            if self.running_realtime is not None:
                self.is_realtime_showed = True

        if sig_calc_dict["is_it_all"]:
            self.statusBar().showMessage(f"Calculation \"{self.results_tabs[rs_id].tab_name}\" is complete.")

            # restore buttons
            self.butt_abort.setEnabled(False)
            self.butt_start.setEnabled(True)
        else:
            self.statusBar().showMessage(f"Overall plot in calculation \"{self.results_tabs[rs_id].tab_name}\" "
                                         f"is complete. Animation figures are now interpolated...")
            self.results_tabs[rs_id].change_no_anim_notification(still_interpolating=True)

        # return progress bar to default state
        self.status_prg_bar.setValue(0)

    # show animation results from calculation, called from signal
    def show_animation_results(self, sig_calc_dict: dict):
        # get ID results tab
        rs_id = sig_calc_dict["id"]

        # show animation data in results tab
        self.results_tabs[rs_id].render_first_animation_fig(sig_calc_dict["x_grid"],
                                                            sig_calc_dict["y_grid"],
                                                            sig_calc_dict["rain_grids"],
                                                            sig_calc_dict["calc_data"])

        # return progress bar to default state
        self.status_prg_bar.setValue(0)

        if self.running_realtime is not None:
            timediff = datetime.now() - self.realtime_last_run
            interval = self.results_tabs[rs_id].cp['output_step'] * 60 + 10

            if timediff.total_seconds() < interval:
                self.butt_abort.setEnabled(True)
                msg = str(f"Realtime iteration #{self.running_realtime.realtime_runs} of calculation "
                          f"\"{self.results_tabs[rs_id].tab_name}\" is complete. "
                          f"Next iteration starts in {int(interval - timediff.total_seconds())} seconds.")
                self.realtime_timer.start(int(interval - timediff.total_seconds()) * 1000)
            else:
                msg = str(f"Processing realtime calculation iteration #{self.running_realtime.realtime_runs}...")
                self._pool_realtime_run()

        else:
            self.butt_start.setEnabled(True)
            msg = str(f"Calculation \"{self.results_tabs[rs_id].tab_name}\" is complete.")

        del sig_calc_dict
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
            logger.warning(msg)
            self.statusBar().showMessage(msg)
            return

        # check if InfluxDB manager is not locked by writing of outputs
        if influx_man.is_manager_locked:
            msg = "Cannot start new calculation, writing of previous outputs is still in progress."
            logger.warning(msg)
            self.statusBar().showMessage(msg)
            return

        # create dict with calculation parameters
        cp = self._get_calc_params()

        # for writing output data back into DB, we need working MariaDB connection
        if cp['is_output_write'] and self.sql_status != 1:
            msg = "Cannot start realtime calculation, MariaDB connection is not available."
            logger.warning(msg)
            self.statusBar().showMessage(msg)
            return

        # INPUT CHECKS:
        if cp['time_diff'] < 0:   # if timediff is less than 1 hour (in msecs)
            msg = "Bad input! Entered bigger (or same) start date than end date!"
            logger.warning(msg)
        elif cp['time_diff'] < 3600000:
            msg = "Bad input! Time difference between start and end times must be at least 1 hour."
            logger.warning(msg)
        elif (cp['time_diff'] / (cp['step'] * 60000)) < 12:
            # TODO: load magic constant 12 from options
            msg = "Bad input! Data resolution must be at least 12 times lower than input time interval length."
            logger.warning(msg)
        elif cp['rolling_values'] < 6:
            # TODO: load magic constant 6 from options
            msg = f"Rolling time window length must be, for these times, at least {(cp['step'] * 6) / 60} hours."
            logger.warning(msg)
        elif (cp['rolling_hours'] * 3600000) > cp['time_diff']:
            msg = f"Rolling time window length cannot be longer than set time interval."
            logger.warning(msg)
        elif cp['output_step'] < cp['step']:
            msg = f"Output frame interval cannot be shorter than initial data resolution."
            logger.warning(msg)
        elif cp['step'] > 59:
            msg = f"Input time interval cannot be longer than 59 minutes."
            logger.warning(msg)
        elif cp['is_cnn_enabled'] and \
                ((cp['start'].secsTo(cp['end']) / 60) / cp['step']) <= CNN_OUTPUT_LEFT_NANS_LENGTH:
            msg = f"When using CNN with {cp['step']}m resolution, " \
                  f"input time interval must be at least {(CNN_OUTPUT_LEFT_NANS_LENGTH * cp['step']) / 60} hours long."
            logger.warning(msg)
        else:
            self.result_id += 1

            # create calculation instance
            calculation = Calculation(
                influx_man,
                self.calc_signals,
                self.result_id,
                self.links,
                self.current_selection,
                cp
            )

            if self.results_name.text() == "":
                results_tab_name = "<no name>"
            else:
                results_tab_name = self.results_name.text()

            # create results widget instance and prepare outputs write, if activated
            if cp['is_output_write']:
                start_time = datetime.utcnow()
                output_delta = timedelta(minutes=cp['output_step'])
                since_time = start_time - output_delta

                params = sql_man.get_last_realtime()
                logger.info("Calculation realtime outputs writing activated!")
                if len(params) != 0:
                    logger.info(
                        "Last written calculation started at %s and ran with parameters: "
                        "retention: %.0f h, step: %.0f min, grid resolution: %.8f °.",
                        params['start_time'].strftime('%Y-%m-%d %H:%M:%S'),
                        params['retention'] / 60, params['timestep'] / 60, params['resolution']
                    )
                logger.info(
                    "Current parameters are: retention: %d h, step: %d min, grid resolution: %.8f °.",
                    cp['retention'], cp['output_step'], cp['interpol_res']
                )

                # if force write is activated, rewrite history...
                influx_wipe_thread = None
                if cp['is_force_write']:
                    logger.info("[DEVMODE] FORCE write activated, all calculations will be ERASED from the database.")
                    # 1) purge realtime data from MariaDB
                    sql_man.wipeout_realtime_tables()
                    # 2) start thread for wiping out output bucket in InfluxDB, it can take a while
                    # - store the thread reference and pass it into the RealtimeWriter,
                    # - since it must be joined before new Influx writes can be made
                    influx_wipe_thread = influx_man.run_wipeout_output_bucket()
                    # 3) purge raw .npy raingrid outputs from disk
                    purge_raw_outputs()
                    logger.info("[DEVMODE] ERASE DONE. New calculation data will be written.")

                realtime_w = RealtimeWriter(
                    sql_man,
                    influx_man,
                    cp['is_history_write'],
                    cp['is_influx_write_skipped'],
                    since_time,
                    influx_wipe_thread=influx_wipe_thread
                )

                self.results_tabs[self.result_id] = ResultsWidget(
                    results_tab_name,
                    self.result_id,
                    self.outputs_path,
                    cp,
                    realtime_writer=realtime_w
                )

                sql_man.insert_realtime(
                    cp['retention'] * 60,
                    cp['output_step'] * 60,
                    cp['interpol_res'],
                    cp['X_MIN'], cp['X_MAX'], cp['Y_MIN'], cp['Y_MAX']
                )
            else:
                self.results_tabs[self.result_id] = ResultsWidget(
                    results_tab_name,
                    self.result_id,
                    self.outputs_path,
                    cp,
                    realtime_writer=None
                )

            self.results_name.clear()
            self.butt_start.setEnabled(False)

            if cp['is_realtime']:
                calculation.setAutoDelete(False)
                self.running_realtime = calculation
                self._pool_realtime_run()
                msg = "Processing realtime calculation iteration..."
            else:
                # run calculation on worker thread from threadpool
                self.threadpool.start(calculation)
                # calculation.run()  # DEBUG: run directly on gui thread
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
        self.outputs_path = QFileDialog.getExistingDirectory(self, 'Select folder for outputs', self.outputs_path)
        if self.outputs_path == '':
            self.outputs_path = './outputs'
        self.outputs_path_box.setText(self.outputs_path + '/<time>')

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
        if selection == "<ALL>":
            sel = self.sets_man.linksets["DEFAULT"]
            self.butt_edit_set.setEnabled(False)
            self.butt_copy_set.setEnabled(False)
            self.butt_del_set.setEnabled(False)
        else:
            sel = self.sets_man.linksets[selection]
            self.butt_edit_set.setEnabled(True)
            self.butt_copy_set.setEnabled(True)
            self.butt_del_set.setEnabled(True)

        self.current_selection = {int(link_id): int(sel[link_id]) for link_id in sel}
        visible_row_count = sum(1 for link_id in sel if sel[link_id] != '0')

        try:
            self.link_table_factory.update_table(self.current_selection, visible_row_count, self.links)
        except Exception as e:
            logger.error("Error while updating link table: %s", e)

    def _fill_linksets(self):
        for link_set in self.sets_man.sections:
            self.lists.addItem(link_set)

    # get parameters from Qt widgets and config manager and pass them into dictionary
    def _get_calc_params(self) -> {}:
        start = self.datetime_start.dateTime()
        end = self.datetime_stop.dateTime()
        step = self.spin_timestep.value()
        time_diff = start.msecsTo(end)
        is_cnn_enabled = self.radio_cnn.isChecked()
        is_external_filter_enabled = self.action_external_filter.isChecked()
        rolling_hours = self.spin_roll_window.value()
        rolling_values = int((rolling_hours * 60) / step)
        wet_dry_deviation = self.spin_wet_dry_sd.value()
        baseline_samples = self.spin_baseline_samples.value()
        interpol_res = self.spin_interpol_res.value()
        idw_power = self.spin_idw_power.value()
        idw_near = self.spin_idw_near.value()
        idw_dist = self.spin_idw_dist.value()
        output_step = self.spin_output_step.value()
        min_rain_value = float(config_handler.read_option('rainfields', 'min_value'))
        is_only_overall = self.box_only_overall.isChecked()
        is_output_total = self.radio_output_total.isChecked()
        is_pdf = self.pdf_box.isChecked()
        is_png = self.png_box.isChecked()
        is_dummy = self.check_dummy.isChecked()
        map_file = config_handler.read_option('rendering', 'map')
        animation_speed = int(config_handler.read_option('viewer', 'animation_speed'))
        waa_schleiss_val = self.spin_waa_schleiss_val.value()
        waa_schleiss_tau = self.spin_waa_schleiss_tau.value()
        close_func = self.close_tab_result
        is_temp_compensated = self.compensation_box.isChecked() and self.filter_box.isChecked()
        correlation_threshold = self.correlation_spin.value()
        realtime_timewindow = self.combo_realtime.currentText()
        is_realtime = self.radio_realtime.isChecked()
        is_temp_filtered = self.correlation_filter_box.isChecked() and self.filter_box.isChecked()
        is_output_write = self.write_output_box.isChecked()
        is_history_write = self.write_history_box.isChecked()
        is_force_write = self.force_write_box.isChecked() and self.write_output_box.isChecked()
        is_influx_write_skipped = self.skip_influx_write_box.isChecked()
        is_window_centered = True if self.window_pointer_combo.currentIndex() == 0 else False
        retention = int(config_handler.read_option('realtime', 'retention'))
        X_MIN = float(config_handler.read_option('rendering', 'X_MIN'))
        X_MAX = float(config_handler.read_option('rendering', 'X_MAX'))
        Y_MIN = float(config_handler.read_option('rendering', 'Y_MIN'))
        Y_MAX = float(config_handler.read_option('rendering', 'Y_MAX'))

        if is_external_filter_enabled:
            external_filter_params = {
                'url': config_handler.read_option('external_filter', 'url'),
                'radius': int(config_handler.read_option('external_filter', 'radius')),
                'pixel_threshold': int(config_handler.read_option('external_filter', 'pixel_threshold')),
                'default_return':
                    True if config_handler.read_option('external_filter', 'default_return') == 'True' else False,
                'IMG_X_MIN': float(config_handler.read_option('external_filter', 'IMG_X_MIN')),
                'IMG_X_MAX': float(config_handler.read_option('external_filter', 'IMG_X_MAX')),
                'IMG_Y_MIN': float(config_handler.read_option('external_filter', 'IMG_Y_MIN')),
                'IMG_Y_MAX': float(config_handler.read_option('external_filter', 'IMG_Y_MAX'))
            }
        else:
            external_filter_params = None

        calculation_params = {
            'start': start,
            'end': end,
            'step': step,
            'time_diff': time_diff,
            'is_cnn_enabled': is_cnn_enabled,
            'is_external_filter_enabled': is_external_filter_enabled,
            'external_filter_params': external_filter_params,
            'rolling_hours': rolling_hours,
            'rolling_values': rolling_values,
            'wet_dry_deviation': wet_dry_deviation,
            'baseline_samples': baseline_samples,
            'interpol_res': interpol_res,
            'idw_power': idw_power,
            'idw_near': idw_near,
            'idw_dist': idw_dist,
            'output_step': output_step,
            'min_rain_value': min_rain_value,
            'is_only_overall': is_only_overall,
            'is_output_total': is_output_total,
            'is_pdf': is_pdf,
            'is_png': is_png,
            'is_dummy': is_dummy,
            'map_file': map_file,
            'animation_speed': animation_speed,
            'waa_schleiss_val': waa_schleiss_val,
            'waa_schleiss_tau': waa_schleiss_tau,
            'close_func': close_func,
            'is_temp_compensated': is_temp_compensated,
            'correlation_threshold': correlation_threshold,
            'realtime_timewindow': realtime_timewindow,
            'is_realtime': is_realtime,
            'is_temp_filtered': is_temp_filtered,
            'is_output_write': is_output_write,
            'is_history_write': is_history_write,
            'is_force_write': is_force_write,
            'is_influx_write_skipped': is_influx_write_skipped,
            'is_window_centered': is_window_centered,
            'retention': retention,
            'X_MIN': X_MIN,
            'X_MAX': X_MAX,
            'Y_MIN': Y_MIN,
            'Y_MAX': Y_MAX
        }

        return calculation_params
