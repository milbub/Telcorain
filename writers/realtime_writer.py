from datetime import datetime
import numpy as np

from database.sql_manager import SqlManager
from database.influx_manager import InfluxManager


def dt64_to_unixtime(dt64):
    unix_epoch = np.datetime64(0, 's')
    s = np.timedelta64(1, 's')
    return int((dt64 - unix_epoch) / s)


class RealtimeWriter:
    def __init__(self, sql_man: SqlManager, influx_man: InfluxManager, write_historic: bool, since_time: datetime):
        self.sql_man = sql_man
        self.influx_man = influx_man
        self.write_historic = write_historic
        self.since_time = since_time

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

        # I. RAINGRIDS
        for t in range(len(calc_dataset.time) - 1):
            time = calc_dataset.time[t]
            if (time.values > np_last_time) and (self.write_historic or (time.values > np_since_time)):
                print(f"[OUTPUT WRITE] Writing raingrid {time.values} into database...")
                self.sql_man.insert_raingrid(datetime.utcfromtimestamp(dt64_to_unixtime(time.values)),
                                             calc_dataset.isel(time=t).cml_id.values.tolist(),
                                             np.around(rain_grids[t], decimals=2).tolist())

        print("[OUTPUT WRITE] Writing raingrids - DONE.")

        # II. INDIVIDUAL CMLS
        pass
