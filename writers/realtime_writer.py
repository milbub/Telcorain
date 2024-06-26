from datetime import datetime
from influxdb_client import Point, WritePrecision
import numpy as np

from database.sql_manager import SqlManager
from database.influx_manager import InfluxManager
from procedures.utils.helpers import dt64_to_unixtime


class RealtimeWriter:
    def __init__(
            self,
            sql_man: SqlManager,
            influx_man: InfluxManager,
            write_historic: bool,
            skip_influx: bool,
            since_time: datetime
    ):
        self.sql_man = sql_man
        self.influx_man = influx_man
        self.write_historic = write_historic
        self.skip_influx = skip_influx
        self.since_time = since_time

    def _write_raingrids(self, rain_grids, calc_dataset, np_last_time, np_since_time):
        for t in range(len(calc_dataset.time)):
            time = calc_dataset.time[t]
            if (time.values > np_last_time) and (self.write_historic or (time.values > np_since_time)):
                print(f"[OUTPUT WRITE: MariaDB] Writing raingrid {time.values} into database...")

                raingrid_time = datetime.utcfromtimestamp(dt64_to_unixtime(time.values))
                raingrid_links = calc_dataset.isel(time=t).cml_id.values.tolist()
                raingrid_values = np.around(rain_grids[t], decimals=2)
                raingrid_values = np.nan_to_num(raingrid_values, nan=0).tolist()  # replace NaNs with 0s

                self.sql_man.insert_raingrid(raingrid_time, raingrid_links, raingrid_values)

        print("[OUTPUT WRITE: MariaDB] Writing raingrids - DONE.")

    def _write_timeseries(self, calc_dataset, np_last_time, np_since_time):
        if not self.write_historic and (np_since_time > np_last_time):
            compare_time = np_since_time
        else:
            compare_time = np_last_time

        points_to_write = []

        print("[OUTPUT WRITE: InfluxDB] Preparing rain timeseries from individual CMLs for writing into database...")

        filtered = calc_dataset.where(calc_dataset.time > compare_time).dropna(dim='time', how='all')
        cmls_count = filtered.cml_id.size
        times_count = filtered.time.size

        if (cmls_count > 0) and (times_count > 0):
            for cml in range(cmls_count):
                for time in range(times_count):
                    points_to_write.append(
                        Point('telcorain')
                        .tag('cml_id', int(filtered.isel(cml_id=cml).cml_id))
                        .field(
                            "rain_intensity",
                            float(filtered.isel(cml_id=cml).R.mean(dim='channel_id').isel(time=time))
                        )
                        .time(
                            dt64_to_unixtime(filtered.isel(time=time).time.values),
                            write_precision=WritePrecision.S
                        )
                    )

        print("[OUTPUT WRITE: InfluxDB] Writing rain timeseries from individual CMLs into database...")
        self.influx_man.write_points(points_to_write, self.influx_man.BUCKET_OUT_CML)
        print("[OUTPUT WRITE: InfluxDB] Writing rain timeseries from individual CMLs - DONE.")

    def push_results(self, rain_grids, calc_dataset):
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

        # I. RAINGRIDS INTO MARIADB
        self._write_raingrids(rain_grids, calc_dataset, np_last_time, np_since_time)
        del rain_grids

        # II. INDIVIDUAL CML TIMESERIES INTO INFLUXDB (if not skipped)
        if not self.skip_influx:
            self._write_timeseries(calc_dataset, np_last_time, np_since_time)

        del calc_dataset
