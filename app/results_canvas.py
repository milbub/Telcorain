import matplotlib
from matplotlib import pyplot
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg
from matplotlib.figure import Figure

matplotlib.use('QtAgg')


class Canvas(FigureCanvasQTAgg):
    def __init__(
            self,
            x_min: float,
            x_max: float,
            y_min: float,
            y_max: float,
            map_bg: str,
            dpi: int = 96,
            left: float = 0,
            bottom: float = 0.03,
            right: float = 1,
            top: float = 0.97
    ):
        # setup single plot positioning
        self.fig = Figure(dpi=dpi)
        self.fig.tight_layout()
        self.ax = self.fig.add_subplot(111, xlim=(x_min, x_max), ylim=(y_min, y_max))
        self.ax.axes.xaxis.set_visible(False)
        self.ax.axes.yaxis.set_visible(False)
        self.fig.subplots_adjust(left, bottom, right, top)

        bg_map = pyplot.imread(f"./assets/{map_bg}")
        self.ax.imshow(bg_map, zorder=0, extent=(x_min, x_max, y_min, y_max), aspect='auto')

        super(Canvas, self).__init__(self.fig)

        self.pc = None
        self.cbar = None

        # TODO: remove this and implement proper value printing when clinking on a map
        # TEST
        def onclick(event):
            print(event)

        self.mpl_connect('button_press_event', onclick)
