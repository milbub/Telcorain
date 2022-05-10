import os
import webbrowser

import matplotlib
from PyQt6 import uic
from PyQt6.QtCore import QDateTime, QTimer
from PyQt6.QtWidgets import QWidget, QLabel, QGridLayout, QSlider, QPushButton, QMessageBox
from matplotlib import cm, colors, pyplot
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg
from matplotlib.figure import Figure

matplotlib.use('QtAgg')


class Canvas(FigureCanvasQTAgg):
    # TODO: load from options
    # rendered area borders
    X_MIN = 14.21646819
    X_MAX = 14.70604375
    Y_MIN = 49.91505682
    Y_MAX = 50.22841327

    def __init__(self, dpi=96, left=0, bottom=0.03, right=1, top=0.97):
        # setup single plot positioning
        self.fig = Figure(dpi=dpi)
        self.fig.tight_layout()
        self.ax = self.fig.add_subplot(111, xlim=(self.X_MIN, self.X_MAX), ylim=(self.Y_MIN, self.Y_MAX))
        self.ax.axes.xaxis.set_visible(False)
        self.ax.axes.yaxis.set_visible(False)
        self.fig.subplots_adjust(left, bottom, right, top)

        # TODO: load path from options
        bg_map = pyplot.imread('./maps/prague_35x35.png')
        self.ax.imshow(bg_map, zorder=0, extent=(self.X_MIN, self.X_MAX, self.Y_MIN, self.Y_MAX), aspect='auto')

        super(Canvas, self).__init__(self.fig)

        self.pc = None
        self.cbar = None


def _plot_link_lines(links_data, ax):
    ax.plot([links_data.site_a_longitude, links_data.site_b_longitude],
            [links_data.site_a_latitude, links_data.site_b_latitude],
            'k', linewidth=1)


class ResultsWidget(QWidget):
    # animation speed constant, later speed control can be implemented
    ANIMATION_SPEED = 1000

    def __init__(self, tab_name: str, result_id: int, start: QDateTime, end: QDateTime, output_step: int,
                 are_results_totals: bool, figs_path: str, is_pdf: bool, is_png: bool, tab_close, is_overall: bool):
        super(QWidget, self).__init__()
        self.tab_name = tab_name
        self.result_id = result_id
        self.start = start
        self.end = end
        self.output_step = output_step
        self.are_results_totals = are_results_totals
        self.figs_path = figs_path
        self.is_pdf = is_pdf
        self.is_png = is_png
        self.tab_close = tab_close
        self.is_only_overall = is_overall

        # saves info
        self.figs_full_path = ''
        self.figs_save_info = {-1: False}

        # load UI definition from Qt XML file
        uic.loadUi("./gui/ResultsWidget.ui", self)

        # lookup for used widgets and define them
        self.overall_plot_layout = self.findChild(QGridLayout, "layoutOverallPlot")
        self.main_plot_layout = self.findChild(QGridLayout, "layoutMainPlot")
        self.tab_name_label = self.findChild(QLabel, "labelCalcName")
        self.label_no_anim_notify = self.findChild(QLabel, "labelNoAnim")
        self.label_current_fig_time = self.findChild(QLabel, "labelCurrentFig")
        self.slider = self.findChild(QSlider, "sliderFrames")
        self.button_play_pause = self.findChild(QPushButton, "buttPlayPause")
        self.button_prev = self.findChild(QPushButton, "buttPrev")
        self.button_next = self.findChild(QPushButton, "buttNext")
        self.button_start = self.findChild(QPushButton, "buttStart")
        self.button_end = self.findChild(QPushButton, "buttEnd")
        self.butt_save = self.findChild(QPushButton, "buttSave")
        self.butt_open = self.findChild(QPushButton, "buttOpenFolder")
        self.butt_close = self.findChild(QPushButton, "buttClose")

        # connect buttons
        self.button_play_pause.clicked.connect(self.start_pause_fired)
        self.button_prev.clicked.connect(self.prev_animation_fig)
        self.button_next.clicked.connect(self.next_animation_fig)
        self.button_start.clicked.connect(self.first_animation_fig)
        self.button_end.clicked.connect(self.last_animation_fig)
        self.butt_save.clicked.connect(self.save_fired)
        self.butt_open.clicked.connect(self.open_folder_fired)
        self.butt_close.clicked.connect(self.close_tab_fired)

        # display info
        self.tab_name_label.setText(tab_name)

        # setup colormap for plots
        self.rain_cmap = cm.get_cmap('turbo', 15)
        self.rain_cmap.set_under('k', alpha=0)

        # prepare canvases
        self.overall_canvas = Canvas(dpi=75)
        self.animation_canvas = Canvas(dpi=75)

        # declare animation rain grids
        self.animation_grids = []
        self.animation_x_grid = None
        self.animation_y_grid = None

        # init animation counter
        self.animation_counter = 0
        self.current_anim_time = start

        # init animation slider
        self.slider_return_to_anim = False
        self.slider.sliderPressed.connect(self._slider_pressed)
        self.slider.sliderMoved.connect(self._slider_moved)
        self.slider.sliderReleased.connect(self._slider_released)

        # init animation timer
        self.animation_timer = QTimer()  # create timer for next checks
        self.animation_timer.timeout.connect(self.next_animation_fig)

    def change_no_anim_notification(self, still_interpolating: bool):
        if still_interpolating:
            self.butt_save.setEnabled(False)
            self.label_no_anim_notify.setText("Animation figures are being interpolated...")
        else:
            self.label_no_anim_notify.hide()
            self.butt_save.setEnabled(True)

    # called from signal
    def render_overall_fig(self, x_grid, y_grid, rain_grid, links_calc_data):
        # render rainfall total
        self._refresh_fig(self.overall_canvas, x_grid, y_grid, rain_grid, is_total=True)

        # plot link path lines
        _plot_link_lines(links_calc_data, self.overall_canvas.ax)

        # show in overall canvas frame
        self.overall_plot_layout.addWidget(self.overall_canvas)

    # called from signal
    def render_first_animation_fig(self, x_grid, y_grid, rain_grids, links_calc_data):
        self.animation_grids = rain_grids
        self.animation_x_grid = x_grid
        self.animation_y_grid = y_grid

        # render first figure of the animation
        self._refresh_fig(self.animation_canvas, x_grid, y_grid, rain_grids[0], is_total=self.are_results_totals)

        # plot link path lines
        _plot_link_lines(links_calc_data, self.animation_canvas.ax)

        # hide notification
        self.change_no_anim_notification(False)

        # show in animation canvas frame
        self.main_plot_layout.addWidget(self.animation_canvas)

        # update time
        self._update_animation_time()

        # init slider
        self.slider.setMaximum(len(rain_grids) - 1)

        # unlock animation controls
        self._set_enabled_controls(True)

    def start_pause_fired(self):
        if self.animation_timer.isActive():
            self.button_play_pause.setText('⏵')
            self.animation_timer.stop()
            self.slider.setEnabled(True)
        else:
            self.slider.setEnabled(False)
            self.button_play_pause.setText('⏸')
            self.animation_timer.start(self.ANIMATION_SPEED)

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
            if self.is_pdf:
                dialog = QMessageBox(self)
                dialog.setWindowTitle("Notice")
                dialog.setText('PDF saves take several seconds in current implementation.')
                dialog.setStandardButtons(QMessageBox.StandardButton.Ok)
                dialog.setIcon(QMessageBox.Icon.Information)
                dialog.exec()

            current_time = QDateTime.currentDateTime().toString("yyyy-MM-dd_HH-mm-ss")
            self.figs_full_path = f'{self.figs_path}/{current_time}'
            os.makedirs(self.figs_full_path, exist_ok=True)

            overall_file = self.start.toString("yyyy-MM-dd_HH-mm-ss") + '_to_' + self.end.toString("yyyy-MM-dd_HH-mm-ss")
            self._save_figs(self.overall_canvas, overall_file, 120)

            self.butt_open.setEnabled(True)
            self.figs_save_info[-1] = True

        if len(self.animation_grids) > 0:
            current_file = self.current_anim_time.toString("yyyy-MM-dd_HH-mm-ss")
            if self.are_results_totals:
                current_file = current_file + f'_{self.output_step}m_total'
            else:
                current_file = current_file + f'_{self.output_step}m_mean_R'

            self._save_figs(self.animation_canvas, current_file, 96)
            self.figs_save_info[self.animation_counter] = True

        if not self.is_only_overall:
            self._set_enabled_controls(True)

    def open_folder_fired(self):
        # must use webbrowser module, since it's only multiplatform solution
        webbrowser.open(os.path.realpath(self.figs_full_path))

    def close_tab_fired(self):
        self.tab_close(self.result_id)

    def _update_save_button(self):
        if self.animation_counter not in self.figs_save_info:
            self.figs_save_info[self.animation_counter] = False

        if self.figs_save_info[self.animation_counter]:
            self.butt_save.setEnabled(False)
        else:
            self.butt_save.setEnabled(True)

    def _save_figs(self, canvas, file: str, dpi: int):
        if self.is_png:
            canvas.print_figure(filename=self.figs_full_path + '/' + file + '.png', format='png',
                                dpi=dpi, bbox_inches='tight', pad_inches=0.3)
        if self.is_pdf:
            canvas.print_figure(filename=self.figs_full_path + '/' + file + '.pdf', format='pdf',
                                dpi=dpi, bbox_inches='tight', pad_inches=0.3)

    def _update_animation_time(self):
        self.current_anim_time = self.start.addSecs(self.output_step * (self.animation_counter + 1) * 60)
        self.label_current_fig_time.setText(self.current_anim_time.toString("dd.MM.yyyy HH:mm:ss"))

    def _update_animation_fig(self):
        self._refresh_fig(self.animation_canvas, self.animation_x_grid, self.animation_y_grid,
                          self.animation_grids[self.animation_counter], is_total=self.are_results_totals)
        self.animation_canvas.draw()

    def _refresh_fig(self, canvas, x_grid, y_grid, rain_grid, is_total: bool = False):
        # clear old plots
        if canvas.cbar is not None:
            canvas.pc.colorbar.remove()
            del canvas.cbar
        if canvas.pc is not None:
            canvas.pc.remove()
            del canvas.pc

        canvas.pc = canvas.ax.pcolormesh(x_grid, y_grid, rain_grid, norm=colors.LogNorm(vmin=0.1, vmax=100),
                                         shading='nearest', cmap=self.rain_cmap, alpha=0.75)
        if is_total:
            canvas.cbar = canvas.fig.colorbar(canvas.pc, format='%d', label='Rainfall Total (mm)')
        else:
            canvas.cbar = canvas.fig.colorbar(canvas.pc, format='%d', label='Rainfall Intensity (mm/h)')

        canvas.cbar.draw_all()

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
            self.animation_timer.start(self.ANIMATION_SPEED)

    def _set_enabled_controls(self, enabled: bool):
        self.button_play_pause.setEnabled(enabled)
        self.button_prev.setEnabled(enabled)
        self.button_next.setEnabled(enabled)
        self.button_start.setEnabled(enabled)
        self.button_end.setEnabled(enabled)
        self.slider.setEnabled(enabled)
