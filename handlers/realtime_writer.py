from datetime import datetime
from threading import Thread
from typing import Optional

from influxdb_client import Point, WritePrecision
import numpy as np
from xarray import Dataset

from database.sql_manager import SqlManager
from database.influx_manager import InfluxManager
from procedures.utils.helpers import dt64_to_unixtime
from handlers.logging_handler import logger


class RealtimeWriter:
    def __init__(
            self,
            sql_man: SqlManager,
            influx_man: InfluxManager,
            write_historic: bool,
            skip_influx: bool,
            since_time: datetime,
            influx_wipe_thread: Optional[Thread] = None
    ):
        self.sql_man = sql_man
        self.influx_man = influx_man
        self.write_historic = write_historic
        self.skip_influx = skip_influx
        self.since_time = since_time
        self.influx_wipe_thread = influx_wipe_thread

    def _write_raingrids(self, rain_grids, calc_dataset, np_last_time, np_since_time):
        for t in range(len(calc_dataset.time)):
            time = calc_dataset.time[t]
            if (time.values > np_last_time) and (self.write_historic or (time.values > np_since_time)):
                formatted_time = datetime.utcfromtimestamp(dt64_to_unixtime(time.values)).strftime("%Y-%m-%d %H:%M")
                logger.info("[WRITE: MariaDB] Writing raingrid %s into database...", formatted_time)

                raingrid_time = datetime.utcfromtimestamp(dt64_to_unixtime(time.values))
                raingrid_links = calc_dataset.isel(time=t).cml_id.values.tolist()
                raingrid_values = np.around(rain_grids[t], decimals=2)
                raingrid_values = np.nan_to_num(raingrid_values, nan=0).tolist()  # replace NaNs with 0s

                self.sql_man.insert_raingrid(raingrid_time, raingrid_links, raingrid_values)

        logger.info("[WRITE: MariaDB] Writing raingrids - DONE.")

    def _write_timeseries(self, calc_dataset, np_last_time, np_since_time):
        if not self.write_historic and (np_since_time > np_last_time):
            compare_time = np_since_time
        else:
            compare_time = np_last_time

        points_to_write = []

        logger.info("[WRITE: InfluxDB] Preparing rain timeseries from individual CMLs for "
                    "writing into database...")

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
        if self.influx_wipe_thread is not None:
            logger.debug("[WRITE: InfluxDB] Force write is active. Checking if InfluxDB wipe thread is done...")
            self.influx_wipe_thread.join()
            logger.debug("[WRITE: InfluxDB] Wipe thread is done. Proceeding with timeseries writing...")
        logger.info("[WRITE: InfluxDB] Writing rain timeseries from individual CMLs into database...")
        self.influx_man.write_points(points_to_write, self.influx_man.BUCKET_OUT_CML)
        logger.info("[WRITE: InfluxDB] Writing rain timeseries from individual CMLs - DONE.")

    def push_results(self, rain_grids: list[np.ndarray], calc_dataset: Dataset):
        # lock the influx manager to prevent start of new calculation before writing is finished
        # (lock is checked by the calculation starting mechanism, if the lock is active, calculation cannot start)
        self.influx_man.is_manager_locked = True

        if len(rain_grids) != len(calc_dataset.time):
            logger.error("[ERROR] Cannot write raingrids into DB! Inconsistent count of rain grid frames "
                         "(%d) and times in calculation dataset (%d)!", len(rain_grids), len(calc_dataset.time))
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

        # unlock the influx manager
        self.influx_man.is_manager_locked = False
    def start_push_results_thread(self, rain_grids: list[np.ndarray], calc_dataset: Dataset) -> Thread:
        thread = Thread(target=self.push_results, args=(rain_grids, calc_dataset))
        thread.start()
        return thread
