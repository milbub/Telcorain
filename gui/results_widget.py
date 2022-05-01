import matplotlib
from PyQt6 import uic
from PyQt6.QtWidgets import QWidget, QLabel, QGridLayout
from matplotlib import pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg
from matplotlib.figure import Figure

matplotlib.use('Qt5Agg')


class MatplotCanvas(FigureCanvasQTAgg):
    def __init__(self, parent=None, dpi=96, left=0, bottom=0.03, right=1, top=0.97):
        # setup single plot positioning
        self.fig = Figure(dpi=dpi)
        self.fig.tight_layout()
        self.ax = self.fig.add_subplot(111)
        self.fig.subplots_adjust(left, bottom, right, top)
        super(MatplotCanvas, self).__init__(self.fig)


def _plot_mwlink_lines(ds_cmls, ax):
    ax.plot([ds_cmls.site_a_longitude, ds_cmls.site_b_longitude],
            [ds_cmls.site_a_latitude, ds_cmls.site_b_latitude],
            'k', linewidth=1)


class ResultsWidget(QWidget):
    def __init__(self, tab_name: str):
        super(QWidget, self).__init__()

        # load UI definition from Qt XML file
        uic.loadUi("./gui/ResultsWidget.ui", self)

        # lookup for used widgets and define them
        self.main_plot_frame = self.findChild(QGridLayout, "layoutMainPlot")
        self.tab_name_label = self.findChild(QLabel, "labelCalcName")

        # display tab name
        self.tab_name_label.setText(tab_name)

    def update_main_plot(self, interpolator, rain_grid, cmls_rain_1h):
        main_canvas = MatplotCanvas(self, dpi=75)

        # render rainfall total
        pc = main_canvas.ax.pcolormesh(interpolator.xgrid, interpolator.ygrid, rain_grid, shading='nearest',
                                       cmap=plt.get_cmap('turbo', 15))
        _plot_mwlink_lines(cmls_rain_1h, ax=main_canvas.ax)
        cbar = main_canvas.fig.colorbar(pc, label='Rainfall Total [mm]')
        pc.set_clim(vmin=0.1, vmax=50)
        cbar.draw_all()

        self.main_plot_frame.addWidget(main_canvas)
