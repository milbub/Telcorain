import sys

from PyQt6 import uic, QtGui, QtCore
from PyQt6.QtCore import QTimer
from PyQt6.QtGui import QPixmap, QAction
from PyQt6.QtWidgets import QMainWindow, QLabel, QProgressBar, QHBoxLayout, QWidget, QTextEdit, QListWidget, \
    QDateTimeEdit, QPushButton, QSpinBox, QTabWidget, QLineEdit, QDoubleSpinBox, QRadioButton, QCheckBox, \
    QListWidgetItem, QTableWidget, QGridLayout, QMessageBox, QFileDialog, QApplication, QComboBox

import input.influx_manager as influx
import input.sqlite_manager as sqlite
import procedures.calculation as calc
import writers.linksets_manager as setsman
import writers.log_manager as logger
from gui.form_dialog import FormDialog
from gui.selection_dialog import SelectionDialog
from gui.results_widget import ResultsWidget


# TODO: move Control Tab elements into separate widget. Currently, this class contains main logic + Control Tab widgets.

class MainWindow(QMainWindow):
    def __init__(self):
        super(MainWindow, self).__init__()

        # ////// GUI CONSTRUCTOR \\\\\\

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
        self.status_prg_bar.setMinimum(0)
        self.status_prg_bar.setMaximum(99)
        self.status_prg_layout.addWidget(self.status_prg_bar)
        self.statusBar().addPermanentWidget(self.status_prg)
        self.statusBar().setContentsMargins(14, 0, 14, 0)

        # lookup for used actions and define them
        self.exit_action = self.findChild(QAction, "actionExit")
        self.exit_action.triggered.connect(QApplication.quit)

        # lookup for used widgets and define them
        self.text_log = self.findChild(QTextEdit, "textLog")
        self.lists = self.findChild(QListWidget, "listLists")
        self.selection_table = self.findChild(QTableWidget, "tableSelection")
        self.butt_new_set = self.findChild(QPushButton, "buttLstNew")
        self.butt_edit_set = self.findChild(QPushButton, "buttLstEdit")
        self.butt_copy_set = self.findChild(QPushButton, "buttLstCopy")
        self.butt_del_set = self.findChild(QPushButton, "buttLstDel")
        self.datetime_start = self.findChild(QDateTimeEdit, "dateTimeStart")
        self.datetime_stop = self.findChild(QDateTimeEdit, "dateTimeStop")
        self.spin_timestep = self.findChild(QSpinBox, "spinTimestep")
        self.butt_start = self.findChild(QPushButton, "buttStart")
        self.butt_abort = self.findChild(QPushButton, "buttAbort")
        self.tabs = self.findChild(QTabWidget, "tabWidget")
        self.results_name = self.findChild(QLineEdit, "resultsNameEdit")
        self.spin_roll_window = self.findChild(QDoubleSpinBox, "spinRollWindow")
        self.spin_wet_dry_sd = self.findChild(QDoubleSpinBox, "spinWetDrySD")
        self.spin_baseline_samples = self.findChild(QSpinBox, "spinBaselineSamples")
        self.spin_output_step = self.findChild(QSpinBox, "spinOutputStep")
        self.spin_interpol_res = self.findChild(QDoubleSpinBox, "spinInterResolution")
        self.spin_idw_power = self.findChild(QSpinBox, "spinIdwPower")
        self.spin_idw_near = self.findChild(QSpinBox, "spinIdwNear")
        self.spin_idw_dist = self.findChild(QDoubleSpinBox, "spinIdwDist")
        self.radio_output_total = self.findChild(QRadioButton, "radioOutputTotal")
        self.box_only_overall = self.findChild(QCheckBox, "checkOnlyOverall")
        self.path_box = self.findChild(QLineEdit, "editPath")
        self.butt_choose_path = self.findChild(QPushButton, "buttChoosePath")
        self.pdf_box = self.findChild(QCheckBox, "checkFilePDF")
        self.png_box = self.findChild(QCheckBox, "checkFilePNG")
        self.check_dummy = self.findChild(QCheckBox, "checkDummy")
        self.spin_waa_schleiss_val = self.findChild(QDoubleSpinBox, "spinSchleissWaa")
        self.spin_waa_schleiss_tau = self.findChild(QDoubleSpinBox, "spinSchleissTau")
        self.is_correlation_box = self.findChild(QCheckBox, "isCorrelationBox")
        self.correlation_spin = self.findChild(QDoubleSpinBox, "correlationSpin")
        self.combo_realtime_box = self.findChild(QComboBox, "comboRealtime")
        self.combo_realtime_box.setHidden(True)
        self.radio_realtime = self.findChild(QRadioButton, "radioRealtime")
        self.radio_historic = self.findChild(QRadioButton, "radioTimeint")


        # declare dictionary for created tabs with calculation results
        # <key: int = result ID, value: ResultsWidget>
        self.results_tabs = {}

        # results tabs ID counter
        self.result_id = 0

        # icon for results tabs
        self.results_icon = QtGui.QIcon()
        self.results_icon.addFile('./gui/icons/explore.png', QtCore.QSize(16, 16))

        # connect buttons
        self.butt_start.clicked.connect(self.calculation_fired)
        self.butt_abort.clicked.connect(self.calculation_cancel_fired)
        self.butt_new_set.clicked.connect(self.new_linkset_fired)
        self.butt_edit_set.clicked.connect(self.edit_linkset_fired)
        self.butt_copy_set.clicked.connect(self.copy_linkset_fired)
        self.butt_del_set.clicked.connect(self.delete_linkset_fired)
        self.butt_choose_path.clicked.connect(self.choose_path_fired)

        # connect other signals
        self.spin_timestep.valueChanged.connect(self._adjust_window)

        # style out link table
        self.selection_table.setColumnWidth(0, 40)
        self.selection_table.setColumnWidth(1, 42)
        self.selection_table.setColumnWidth(2, 42)
        self.selection_table.setColumnWidth(3, 75)
        self.selection_table.setColumnWidth(4, 71)
        self.selection_table.setColumnWidth(5, 75)
        self.selection_table.setColumnWidth(6, 148)

        # show window
        self.show()

        # ////// APP LOGIC CONSTRUCTOR \\\\\\

        # redirect stdout to log handler
        sys.stdout = logger.LogManager(self.text_log)
        print("Telcorain is starting...", flush=True)

        # init threadpool
        self.threadpool = QtCore.QThreadPool()

        # init app logic signaling
        self.influx_signals = influx.InfluxSignals()
        self.calc_signals = calc.CalcSignals()

        # influxDB status signal
        self.influx_signals.ping_signal.connect(self.check_influx_status)

        # rainfall calculation signals
        self.calc_signals.overall_done_signal.connect(self.show_overall_results)
        self.calc_signals.plots_done_signal.connect(self.show_animation_results)
        self.calc_signals.error_signal.connect(self.calculation_error)
        self.calc_signals.progress_signal.connect(self.progress_update)

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

        # init link sets
        self.current_selection = {}   # link channel selection flag: 0=none, 1=A, 2=B, 3=both -> dict: <link_id>: flag
        self.sets_man = setsman.LinksetsManager(self.links)
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

    # show overall results from calculation, called from signal
    def show_overall_results(self, meta_data: dict):
        # plot data in results tab
        self.results_tabs[meta_data["id"]].render_overall_fig(meta_data["x_grid"],
                                                              meta_data["y_grid"],
                                                              meta_data["rain_grid"],
                                                              meta_data["link_data"])

        # insert results tab to tab list
        self.tabs.addTab(self.results_tabs[meta_data["id"]], self.results_icon,
                         f"Results: {self.results_tabs[meta_data['id']].tab_name}")

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

        self.statusBar().showMessage(f"Calculation \"{self.results_tabs[meta_data['id']].tab_name}\" is complete.")

        # return progress bar to default state
        self.status_prg_bar.setValue(0)

        # restore buttons
        self.butt_abort.setEnabled(False)
        self.butt_start.setEnabled(True)

    # show info about error in calculation, called from signal
    def calculation_error(self, meta_data: dict):
        self.statusBar().showMessage(f"Error occurred in calculation \"{self.results_tabs[meta_data['id']].tab_name}\"."
                                     f" See system log for more info.")

        # return progress bar to default state
        self.status_prg_bar.setValue(0)

        # restore buttons
        self.butt_abort.setEnabled(False)
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
        is_correlation = self.is_correlation_box.isChecked()
        spin_correlation = self.correlation_spin.value()
        realtime_time = self.combo_realtime_box.dateTime()
        is_realtime = self.radio_realtime.isChecked()
        is_historic = self.radio_historic.isChecked()

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
        else:
            self.result_id += 1

            # create calculation instance
            calculation = calc.Calculation(self.calc_signals, self.result_id, self.links, self.current_selection, start,
                                           end, step, rolling_values, output_step, is_only_overall, is_output_total,
                                           wet_dry_deviation, baseline_samples, interpol_res, idw_power, idw_near,
                                           idw_dist, waa_schleiss_val, waa_schleiss_tau, is_correlation, spin_correlation,
                                           realtime_time, is_realtime, is_historic)

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
            self.results_tabs[self.result_id] = ResultsWidget(results_tab_name, self.result_id, start, end, output_step,
                                                              is_output_total, self.path, is_pdf, is_png, close_func,
                                                              is_only_overall, is_dummy, params)

            self.results_name.clear()
            self.butt_abort.setEnabled(True)
            self.butt_start.setEnabled(False)

            # RUN calculation on worker thread from threadpool
            #self.threadpool.start(calculation)
            calculation.run()  # TEMP: run directly on gui thread for debugging reasons

            msg = "Processing..."

        self.statusBar().showMessage(msg)

    def calculation_cancel_fired(self):
        # TODO: implement calculation cancelling
        pass

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

    """
    def certain_time(self):

        # Create the Historic checkbox
        #self.historic_checkbox = QtWidgets.QCheckBox("Historic", self)
        self.radio_historic.stateChanged.connect(self.on_historic_toggled)

        # Create the Realtime checkbox
        #self.realtime_checkbox = QtWidgets.QCheckBox("Realtime", self)
        self.radio_realtime.stateChanged.connect(self.on_realtime_toggled)

        # Set up the Past min QDateTimeEdit widget
        #self.past_min_edit = QtWidgets.QDateTimeEdit(self)
        self.combo_realtime_box.setDateTime(QComboBox.currentDateTime().addSecs(-300))
        #self.combo_realtime_box.setDisplayFormat("dd.MM.yyyy HH:mm:ss")
        self.combo_realtime_box.setVisible(False)

    def on_historic_toggled(self):
        if self.radio_historic:
            self.datetime_start.setVisible(True)
            self.datetime_stop.setVisible(True)
        else:
            self.datetime_start.setVisible(False)
            self.datetime_stop.setVisible(False)

    def on_realtime_toggled(self):
        if self.radio_realtime:
            self.datetime_start.setVisible(False)
            self.datetime_stop.setVisible(False)
            self.combo_realtime_box.setVisible(True)
        else:
            self.combo_realtime_box.setVisible(False)
        """

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

    # adjust wet/dry rolling window length when query timestep is changed, to default multiple of 36 (good results)
    def _adjust_window(self, step: int):
        self.spin_roll_window.setValue(step * 36 / 60)

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
        for link_set in self.sets_man.set_names:
            self.lists.addItem(link_set)

    # destructor
    def __del__(self):
        # return stdout to default state
        sys.stdout = sys.__stdout__
