import gc
import os
from typing import Optional, cast
import webbrowser

import matplotlib
from matplotlib import cm, colors
from PyQt6 import uic, QtCore
from PyQt6.QtCore import QDateTime, QTimeZone, QTimer
from PyQt6.QtWidgets import QWidget, QLabel, QGridLayout, QSlider, QPushButton, QMessageBox, QTableWidget

from app.results_canvas import Canvas
from procedures.utils.helpers import dt64_to_unixtime
from handlers.realtime_writer import RealtimeWriter

matplotlib.use('QtAgg')


class ResultsWidget(QWidget):
    def __init__(
            self,
            tab_name: str,
            result_id: int,
            figs_path: str,
            cp: dict,
            realtime_writer: Optional[RealtimeWriter]
    ):
        super(QWidget, self).__init__()
        self.tab_name = tab_name
        self.result_id = result_id
        self.figs_path = figs_path

        # calculation parameters dictionary
        self.cp = cp

        # DB realtime writer handler
        self.realtime_writer = realtime_writer

        # saves info
        self.figs_full_path = ''
        self.figs_save_info = {-1: False}   # -1 = overall fig, then 0+ corresponds with animation counter

        # load UI definition from Qt XML file
        uic.loadUi("./app/gui/ResultsWidget.ui", self)

        # lookup for used widgets and define them
        self.overall_plot_layout: QGridLayout = cast(QGridLayout, self.findChild(QGridLayout, "layoutOverallPlot"))
        self.main_plot_layout: QGridLayout = cast(QGridLayout, self.findChild(QGridLayout, "layoutMainPlot"))
        self.tab_name_label: QLabel = cast(QLabel, self.findChild(QLabel, "labelCalcName"))
        self.start_label: QLabel = cast(QLabel, self.findChild(QLabel, "labelStartTime"))
        self.end_label: QLabel = cast(QLabel, self.findChild(QLabel, "labelEndTime"))
        self.interval_label: QLabel = cast(QLabel, self.findChild(QLabel, "labelFrameInterval"))
        self.output_label: QLabel = cast(QLabel, self.findChild(QLabel, "labelOutputType"))
        self.label_no_anim_notify: QLabel = cast(QLabel, self.findChild(QLabel, "labelNoAnim"))
        self.label_current_fig_time: QLabel = cast(QLabel, self.findChild(QLabel, "labelCurrentFig"))
        self.slider: QSlider = cast(QSlider, self.findChild(QSlider, "sliderFrames"))
        self.button_play_pause: QPushButton = cast(QPushButton, self.findChild(QPushButton, "buttPlayPause"))
        self.button_prev: QPushButton = cast(QPushButton, self.findChild(QPushButton, "buttPrev"))
        self.button_next: QPushButton = cast(QPushButton, self.findChild(QPushButton, "buttNext"))
        self.button_start: QPushButton = cast(QPushButton, self.findChild(QPushButton, "buttStart"))
        self.button_end: QPushButton = cast(QPushButton, self.findChild(QPushButton, "buttEnd"))
        self.butt_save: QPushButton = cast(QPushButton, self.findChild(QPushButton, "buttSave"))
        self.butt_open: QPushButton = cast(QPushButton, self.findChild(QPushButton, "buttOpenFolder"))
        self.butt_close: QPushButton = cast(QPushButton, self.findChild(QPushButton, "buttClose"))
        self.table_params: QTableWidget = cast(QTableWidget, self.findChild(QTableWidget, "tableParams"))

        # connect buttons
        self.button_play_pause.clicked.connect(self.start_pause_fired)
        self.button_prev.clicked.connect(self.prev_animation_fig)
        self.button_next.clicked.connect(self.next_animation_fig)
        self.button_start.clicked.connect(self.first_animation_fig)
        self.button_end.clicked.connect(self.last_animation_fig)
        self.butt_save.clicked.connect(self.save_fired)
        self.butt_open.clicked.connect(self.open_folder_fired)
        self.butt_close.clicked.connect(self.close_tab_fired)

        # set tab name
        self.tab_name_label.setText(tab_name)

        # setup colormap for plots
        self.rain_cmap = cm.get_cmap('turbo', 15)
        self.rain_cmap.set_under('k', alpha=0)

        # prepare canvases
        self.overall_canvas = Canvas(cp['X_MIN'], cp['X_MAX'], cp['Y_MIN'], cp['Y_MAX'], cp['map_file'], dpi=75)
        self.animation_canvas = Canvas(cp['X_MIN'], cp['X_MAX'], cp['Y_MIN'], cp['Y_MAX'], cp['map_file'], dpi=75)

        # init animation rain grids list
        self.animation_grids = []
        self.animation_x_grid = None
        self.animation_y_grid = None

        # animation speed constant, later maybe speed control can be implemented?
        self.animation_speed = cp['animation_speed']

        # init link lines list
        self.anim_link_lines = []
        self.overall_link_lines = []

        # we need to track if something is already displayed, so some things can be run only once
        self.is_displayed = False

        # init animation counter
        self.animation_counter = 0
        self.current_anim_time = None

        # init calc start and end time vals
        self.start_time = None
        self.end_time = None
        self.real_start_time = None

        # init animation slider
        self.slider_return_to_anim = False
        self.slider.sliderPressed.connect(self._slider_pressed)
        self.slider.sliderMoved.connect(self._slider_moved)
        self.slider.sliderReleased.connect(self._slider_released)

        # init animation timer
        self.animation_timer = QTimer()  # create timer for next checks
        self.animation_timer.timeout.connect(self.next_animation_fig)

        # fill calculation info
        self._fill_info()

        # point values coordinates (X, Y)
        # TODO: load coordinates from file, implement adding, deleting, editing, ...
        self.points = [(14.352689, 50.080843), (14.4278, 50.0692), (14.4619, 50.0758), (14.5381, 50.1233)]
        self.anim_annotations = []
        self.overall_annotations = []

    def change_no_anim_notification(self, still_interpolating: bool):
        if still_interpolating:
            self.butt_save.setEnabled(False)
            self.label_no_anim_notify.setText("Animation figures are being interpolated...")
        else:
            self.label_no_anim_notify.hide()
            self.butt_save.setEnabled(True)

    # called from signal
    def render_overall_fig(self, start, end, x_grid, y_grid, rain_grid, calc_data):
        # set start and end times
        self._set_times(start, end)

        # render rainfall total
        self._refresh_fig(self.overall_canvas, x_grid, y_grid, rain_grid, self.overall_annotations, is_total=True)

        # plot link path lines
        self._plot_link_lines(calc_data, self.overall_canvas.ax, self.overall_link_lines)

        # add canvas widget only once
        # 'is_displayed' is set to True by first run of 'render_first_animation_fig' after rendering overall fig
        if not self.is_displayed:
            # show in overall canvas frame
            self.overall_plot_layout.addWidget(self.overall_canvas)

        del x_grid
        del y_grid
        del rain_grid
        del calc_data
        gc.collect()

    # called from signal
    def render_first_animation_fig(self, x_grid, y_grid, rain_grids, calc_data):
        del self.animation_grids
        del self.animation_x_grid
        del self.animation_y_grid
        self.animation_grids = rain_grids
        self.animation_x_grid = x_grid
        self.animation_y_grid = y_grid

        # reset start and end times since real start can be different due to skipped frames
        self._set_times(np_real_start=calc_data.isel(time=0).time.values)

        # render first figure of the animation
        self._refresh_fig(self.animation_canvas, x_grid, y_grid, rain_grids[0], self.anim_annotations,
                          is_total=self.cp['is_output_total'])

        # plot link path lines
        self._plot_link_lines(calc_data, self.animation_canvas.ax, self.anim_link_lines)

        # hide notification
        self.change_no_anim_notification(False)

        # add canvas widget only once
        if not self.is_displayed:
            # show in animation canvas frame
            self.main_plot_layout.addWidget(self.animation_canvas)
            # done, next time skip this
            self.is_displayed = True

        # update time
        self._update_animation_time()

        # init slider
        self.slider.setMaximum(len(rain_grids) - 1)

        # unlock animation controls
        self._set_enabled_controls(True)

        # push results into DB
        if self.realtime_writer is not None:
            self.realtime_writer.start_push_results_thread(rain_grids, calc_data)

        del x_grid
        del y_grid
        del rain_grids
        del calc_data
        gc.collect()

    def start_pause_fired(self):
        if self.animation_timer.isActive():
            self.button_play_pause.setText('⏵')
            self.animation_timer.stop()
            self.slider.setEnabled(True)
        else:
            self.slider.setEnabled(False)
            self.button_play_pause.setText('⏸')
            self.animation_timer.start(self.animation_speed)

    def next_animation_fig(self):
        self.animation_counter += 1
        if self.animation_counter < len(self.animation_grids):
            self._update_animation_time()
            self._update_save_button()
            self.slider.setValue(self.animation_counter)
            self._update_animation_fig()
        else:
            self.first_animation_fig()

    def prev_animation_fig(self):
        self.animation_counter -= 1
        if self.animation_counter > -1:
            self._update_animation_time()
            self._update_save_button()
            self.slider.setValue(self.animation_counter)
            self._update_animation_fig()
        else:
            self.last_animation_fig()

    def first_animation_fig(self):
        self.animation_counter = 0
        self._update_animation_time()
        self._update_save_button()
        self.slider.setValue(self.animation_counter)
        self._update_animation_fig()

    def last_animation_fig(self):
        self.animation_counter = len(self.animation_grids) - 1
        self._update_animation_time()
        self._update_save_button()
        self.slider.setValue(self.animation_counter)
        self._update_animation_fig()

    def save_fired(self):
        self.butt_save.setEnabled(False)
        self._set_enabled_controls(False)

        # do only when first time fired
        if not self.figs_save_info[-1]:
            if self.cp['is_pdf']:
                dialog = QMessageBox(self)
                dialog.setWindowTitle("Notice")
                dialog.setText('PDF saves take several seconds in current implementation.')
                dialog.setStandardButtons(QMessageBox.StandardButton.Ok)
                dialog.setIcon(QMessageBox.Icon.Information)
                dialog.exec()

            current_time = QDateTime.currentDateTime().toString("yyyy-MM-dd_HH-mm-ss")
            self.figs_full_path = f'{self.figs_path}/{current_time}'
            os.makedirs(self.figs_full_path, exist_ok=True)

            overall_file = self.cp['start'].toString("yyyy-MM-dd_HH-mm-ss") + '_to_' + \
                           self.cp['end'].toString("yyyy-MM-dd_HH-mm-ss")
            self._save_figs(self.overall_canvas, overall_file, 120)

            self.butt_open.setEnabled(True)
            self.figs_save_info[-1] = True

        if len(self.animation_grids) > 0:
            current_file = self.current_anim_time.toString("yyyy-MM-dd_HH-mm-ss")
            ostp = self.cp['output_step']
            if self.cp['is_output_total']:
                current_file = current_file + f'_{ostp}m_total'
            else:
                current_file = current_file + f'_{ostp}m_mean_R'

            self._save_figs(self.animation_canvas, current_file, 96)
            self.figs_save_info[self.animation_counter] = True

        if not self.cp['is_only_overall']:
            self._set_enabled_controls(True)

    def open_folder_fired(self):
        # must use webbrowser module, since it's only multiplatform solution
        webbrowser.open(os.path.realpath(self.figs_full_path))

    def close_tab_fired(self):
        self.cp['close_func'](self.result_id)

    def _update_save_button(self):
        if self.animation_counter not in self.figs_save_info:
            self.figs_save_info[self.animation_counter] = False

        if self.figs_save_info[self.animation_counter]:
            self.butt_save.setEnabled(False)
        else:
            self.butt_save.setEnabled(True)

    def _save_figs(self, canvas, file: str, dpi: int):
        if self.cp['is_png']:
            canvas.print_figure(filename=self.figs_full_path + '/' + file + '.png', format='png',
                                dpi=dpi, bbox_inches='tight', pad_inches=0.3)
        if self.cp['is_pdf']:
            canvas.print_figure(filename=self.figs_full_path + '/' + file + '.pdf', format='pdf',
                                dpi=dpi, bbox_inches='tight', pad_inches=0.3)

    def _update_animation_time(self):
        self.current_anim_time = self.real_start_time.addSecs(self.cp['output_step'] * self.animation_counter * 60)
        self.label_current_fig_time.setText(self.current_anim_time.toString("dd.MM.yyyy HH:mm:ss"))

    def _update_animation_fig(self):
        self._refresh_fig(self.animation_canvas, self.animation_x_grid, self.animation_y_grid,
                          self.animation_grids[self.animation_counter], self.anim_annotations,
                          is_total=self.cp['is_output_total'])
        self.animation_canvas.draw()

    def _refresh_fig(self, canvas, x_grid, y_grid, rain_grid, annotations, is_total: bool = False):
        # clear old plots
        if canvas.cbar is not None:
            canvas.pc.colorbar.remove()
            del canvas.cbar
        if canvas.pc is not None:
            canvas.pc.remove()
            del canvas.pc
        for annotation in annotations:
            annotation.remove()
        del annotations[:]

        # render raingrid as colormesh
        canvas.pc = canvas.ax.pcolormesh(x_grid, y_grid, rain_grid, norm=colors.LogNorm(vmin=0.1, vmax=100),
                                         shading='nearest', cmap=self.rain_cmap, alpha=0.75)
        if is_total:
            canvas.cbar = canvas.fig.colorbar(canvas.pc, format='%d', label='Rainfall Total (mm)')
        else:
            canvas.cbar = canvas.fig.colorbar(canvas.pc, format='%d', label='Rainfall Intensity (mm/h)')

        canvas.cbar.draw_all()

        # for coords in self.points:
        #     # get 'z' value of a point from rain grid
        #     z = self._get_z_value(rain_grid, coords[0], coords[1])
        #
        #     # plot an annotation and keep its reference
        #     a = canvas.ax.annotate(text='{:.1f}'.format(z), xy=(coords[0], coords[1]), fontsize=14)
        #
        #     # store an annotation reference
        #     annotations.append(a)

    def _plot_link_lines(self, calc_data, ax, link_lines):
        # remove old lines from the plot
        for line in link_lines:
            line.remove()
        link_lines.clear()
        del link_lines[:]

        # plot new lines and keep their references
        if self.cp['is_dummy']:
            new_lines = ax.plot([calc_data.dummy_a_longitude, calc_data.dummy_b_longitude],
                                [calc_data.dummy_a_latitude, calc_data.dummy_b_latitude],
                                'k', linewidth=1)
        else:
            new_lines = ax.plot([calc_data.site_a_longitude, calc_data.site_b_longitude],
                                [calc_data.site_a_latitude, calc_data.site_b_latitude],
                                'k', linewidth=1)

        # store new lines references
        link_lines.extend(new_lines)

    def _get_z_value(self, z_grid, x: float, y: float) -> float:
        x_pos = round((x - self.cp['X_MIN']) * (1 / self.cp['interpol_res']))
        y_pos = round((y - self.cp['Y_MIN']) * (1 / self.cp['interpol_res']))
        return z_grid[y_pos][x_pos]

    def _slider_pressed(self):
        if self.animation_timer.isActive():
            self.animation_timer.stop()
            self.slider_return_to_anim = True
            self.button_play_pause.setText('⏵')

    def _slider_moved(self, pos: int):
        self.animation_counter = pos
        self._update_animation_time()

    def _slider_released(self):
        self._update_save_button()
        self._update_animation_fig()

        if self.slider_return_to_anim:
            self.button_play_pause.setText('⏸')
            self.slider_return_to_anim = False
            self.animation_timer.start(self.animation_speed)

    def _set_enabled_controls(self, enabled: bool):
        self.button_play_pause.setEnabled(enabled)
        self.button_prev.setEnabled(enabled)
        self.button_next.setEnabled(enabled)
        self.button_start.setEnabled(enabled)
        self.button_end.setEnabled(enabled)
        self.slider.setEnabled(enabled)

    def _fill_info(self):
        self.interval_label.setText(str(self.cp['output_step']) + ' minutes')
        if self.cp['is_output_total']:
            self.output_label.setText('Totals (mm)')
        else:
            self.output_label.setText('Intensity (mm/h)')

        table_vals = [QLabel(str(self.cp['rolling_hours'])), QLabel(str(self.cp['wet_dry_deviation'])),
                      QLabel(str(self.cp['baseline_samples'])), QLabel(str(self.cp['interpol_res'])),
                      QLabel(str(self.cp['idw_power'])), QLabel(str(self.cp['idw_near'])),
                      QLabel(str(self.cp['idw_dist'])), QLabel(str(self.cp['waa_schleiss_val'])),
                      QLabel(str(self.cp['waa_schleiss_tau']))]

        for x in range(9):
            table_vals[x].setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            self.table_params.setCellWidget(x, 1, table_vals[x])

    def _set_times(self, start=None, end=None, np_real_start=None):
        if start is not None:
            unix_start = start.item() / 1000000000
            self.start_time = QDateTime.fromSecsSinceEpoch(int(unix_start), QTimeZone.utc())
            self.start_label.setText(self.start_time.toString("dd.MM.yyyy HH:mm"))
            self.current_anim_time = self.start_time

        if end is not None:
            unix_end = end.item() / 1000000000
            self.end_time = QDateTime.fromSecsSinceEpoch(int(unix_end), QTimeZone.utc())
            self.end_label.setText(self.end_time.toString("dd.MM.yyyy HH:mm"))

        if np_real_start is not None:
            unix_real_start = dt64_to_unixtime(np_real_start)
            self.real_start_time = QDateTime.fromSecsSinceEpoch(unix_real_start, QTimeZone.utc())
            self.current_anim_time = self.real_start_time
        else:
            self.real_start_time = self.start_time
