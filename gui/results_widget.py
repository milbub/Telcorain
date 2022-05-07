import matplotlib
from PyQt6 import uic
from PyQt6.QtCore import QDateTime, QTimer
from PyQt6.QtWidgets import QWidget, QLabel, QGridLayout, QSlider, QPushButton
from matplotlib import cm, colors
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg
from matplotlib.figure import Figure

matplotlib.use('QtAgg')


class Canvas(FigureCanvasQTAgg):
    def __init__(self, parent=None, dpi=96, left=0, bottom=0.03, right=1, top=0.97):
        # setup single plot positioning
        self.fig = Figure(dpi=dpi)
        self.fig.tight_layout()
        self.ax = self.fig.add_subplot(111)
        self.ax.axes.xaxis.set_visible(False)
        self.ax.axes.yaxis.set_visible(False)
        self.fig.subplots_adjust(left, bottom, right, top)
        super(Canvas, self).__init__(self.fig)

        self.pc = None
        self.cbar = None


def _plot_link_lines(links_data, ax):
    ax.plot([links_data.site_a_longitude, links_data.site_b_longitude],
            [links_data.site_a_latitude, links_data.site_b_latitude],
            'k', linewidth=1)


class ResultsWidget(QWidget):
    def __init__(self, tab_name: str, start: QDateTime, end: QDateTime, output_step: int, are_results_totals: bool):
        super(QWidget, self).__init__()
        self.tab_name = tab_name
        self.start = start
        self.end = end
        self.output_step = output_step
        self.are_results_totals = are_results_totals

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

        # connect buttons
        self.button_play_pause.clicked.connect(self.start_pause_fired)
        self.button_prev.clicked.connect(self.prev_animation_fig)
        self.button_next.clicked.connect(self.next_animation_fig)
        self.button_start.clicked.connect(self.first_animation_fig)
        self.button_end.clicked.connect(self.last_animation_fig)

        # display tab name
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

        # init animation timer
        self.animation_timer = QTimer()  # create timer for next checks
        self.animation_timer.timeout.connect(self.next_animation_fig)

    def change_no_anim_notification(self, still_interpolating: bool):
        if still_interpolating:
            self.label_no_anim_notify.setText("Animation figures are being interpolated...")
        else:
            self.label_no_anim_notify.hide()

    # called from signal
    def render_overall_fig(self, x_grid, y_grid, rain_grid, links_calc_data):
        # render rainfall total
        self._refresh_fig(self.overall_canvas, x_grid, y_grid, rain_grid, is_total=True)

        # plot link path lines
        _plot_link_lines(links_calc_data, self.overall_canvas.ax)

        # self.overall_canvas.print_figure(filename='./outputs/test.png', dpi=75, format='png')

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

        # unlock animation controls
        self.button_play_pause.setEnabled(True)
        self.button_prev.setEnabled(True)
        self.button_next.setEnabled(True)
        self.button_start.setEnabled(True)
        self.button_end.setEnabled(True)
        self.slider.setEnabled(True)

    def start_pause_fired(self):
        if self.animation_timer.isActive():
            self.button_play_pause.setText('⏵')
            self.animation_timer.stop()
        else:
            self.button_play_pause.setText('⏸')
            # TODO: add speed control
            self.animation_timer.start(1000)

    def next_animation_fig(self):
        self.animation_counter += 1
        if self.animation_counter < len(self.animation_grids):
            self._update_animation_time()
            self._update_animation_fig()
        else:
            self.first_animation_fig()

    def prev_animation_fig(self):
        self.animation_counter -= 1
        if self.animation_counter > -1:
            self._update_animation_time()
            self._update_animation_fig()
        else:
            self.last_animation_fig()

    def first_animation_fig(self):
        self.animation_counter = 0
        self._update_animation_time()
        self._update_animation_fig()

    def last_animation_fig(self):
        self.animation_counter = len(self.animation_grids) - 1
        self._update_animation_time()
        self._update_animation_fig()

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
                                         shading='nearest', cmap=self.rain_cmap)
        if is_total:
            canvas.cbar = canvas.fig.colorbar(canvas.pc, format='%d', label='Rainfall Total (mm)')
        else:
            canvas.cbar = canvas.fig.colorbar(canvas.pc, format='%d', label='Rainfall Intensity (mm/h)')

        canvas.cbar.draw_all()
