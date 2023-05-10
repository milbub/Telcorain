from datetime import datetime
from influxdb_client import Point
from influxdb_client.domain.write_precision import WritePrecision
import numpy as np

from database.sql_manager import SqlManager
from database.influx_manager import InfluxManager


def dt64_to_unixtime(dt64):
    unix_epoch = np.datetime64(0, 's')
    s = np.timedelta64(1, 's')
    return int((dt64 - unix_epoch) / s)


def truncate(f, n) -> str:
    """Truncates/pads a float f to n decimal places without rounding"""
    s = '{}'.format(f)
    if 'e' in s or 'E' in s:
        return '{0:.{1}f}'.format(f, n)
    i, p, d = s.partition('.')
    return '.'.join([i, (d + '0' * n)[:n]])


class RealtimeWriter:
    def __init__(self, sql_man: SqlManager, influx_man: InfluxManager, precision: int, write_historic, since_time):
        self.sql_man = sql_man
        self.influx_man = influx_man
        self.precision = precision
        self.write_historic = write_historic
        self.since_time = since_time

    def push_results(self, x_grid, y_grid, rain_grids, calc_dataset):
        if len(rain_grids) != len(calc_dataset.time):
            print(f"[ERROR] Cannot write raingrids into DB! Inconsistent count of rain grid frames ({len(rain_grids)}) "
                  f"and times in calculation dataset ({len(calc_dataset.time)})!")
            return

        last_record = self.sql_man.get_last_raingrid()
        if len(last_record) > 0:
            last_time = list(last_record.keys())[0]
        else:
            last_time = datetime.min

        np_last_time = np.datetime64(last_time)
        np_since_time = np.datetime64(self.since_time)

        # I. RAINGRIDS
        y_grid_trunc = []
        for y in y_grid:
            y_grid_trunc.append(truncate(y[0], self.precision))

        points_to_write = []
        grid = 0
        for t in range(len(calc_dataset.time) - 1):
            time = calc_dataset.time[t]
            if (time.values > np_last_time) and (self.write_historic or (time.values > np_since_time)):
                print(f"[OUTPUT WRITE] Preparing raingrid {time.values} for writing into database...")

                # Iterate through X coords
                for x in range(0, len(x_grid[0])):
                    x_t = truncate(x_grid[0][x], self.precision)
                    # Iterate through Y coords
                    for y in range(0, len(y_grid)):
                        points_to_write.append(Point('telcorain').tag('x', x_t).tag('y', y_grid_trunc[y])
                                               .field("rain_intensity", rain_grids[grid][y][x])
                                               .time(dt64_to_unixtime(time.values), write_precision=WritePrecision.S))

                print(f"[OUTPUT WRITE] Writing raingrid {time.values} into database...")
                self.influx_man.write_points(points_to_write, self.influx_man.BUCKET_OUT_GRID)
                self.sql_man.insert_raingrid(datetime.utcfromtimestamp(dt64_to_unixtime(time.values)),
                                             calc_dataset.isel(time=grid).cml_id.values.tolist())
            points_to_write = []
            grid += 1

        print("[OUTPUT WRITE] Writing raingrids - DONE.")

        # II. INDIVIDUAL CMLS
        pass
