import datetime

import numpy as np
import xarray as xr
from PyQt6.QtCore import QRunnable

from database.influx_manager import InfluxManager
from procedures.calculation_signals import CalcSignals
from procedures.exceptions import ProcessingException, RaincalcException, RainfieldsGenException
from procedures.data import data_loading, data_preprocessing
from procedures.rain import rain_calculation, rainfields_generation


class Calculation(QRunnable):
    def __init__(
            self,
            influx_man: InfluxManager,
            signals: CalcSignals,
            results_id: int,
            links: dict,
            selection: dict,
            cp: dict
    ):

        QRunnable.__init__(self)
        self.influx_man = influx_man
        self.signals = signals
        self.results_id = results_id
        self.links = links
        self.selection = selection

        # calculation parameters dictionary
        self.cp = cp
        
        # run counter in case of realtime calculation
        self.realtime_runs: int = 0

        # store raingrids for possible next iteration (no need for repeated generating in realtime)
        self.rain_grids = []
        self.last_time = np.datetime64(datetime.datetime.min)

    def run(self):
        self.realtime_runs += 1
        if self.cp['is_realtime']:
            log_run_id = "CALC ID: " + str(self.results_id) + ", RUN: " + str(self.realtime_runs)
        else:
            log_run_id = "CALC ID: " + str(self.results_id)
            
        print(f"[{log_run_id}] Rainfall calculation procedure started.", flush=True)

        try:
            # Gather data from InfluxDB
            influx_data, missing_links, ips = data_loading.load_data_from_influxdb(
                self.influx_man,
                self.signals,
                self.cp,
                self.selection,
                self.links,
                log_run_id,
                self.results_id
            )

            # Merge influx data with metadata into datasets, resolve Tx power assignment to correct channel
            calc_data: list[xr.Dataset] = data_preprocessing.convert_to_link_datasets(
                self.signals,
                self.selection,
                self.links,
                influx_data,
                missing_links,
                log_run_id,
                self.results_id
            )
            del influx_data
        except ProcessingException:
            return

        try:
            # Obtain rain rates and store them in the calc_data
            calc_data = rain_calculation.get_rain_rates(
                self.signals,
                calc_data,
                self.cp,
                ips,
                log_run_id,
                self.results_id
            )
        except RaincalcException:
            return

        try:
            # Generate rainfields (resample rain rates and interpolate them to a grid)
            self.rain_grids, self.realtime_runs, self.last_time = rainfields_generation.generate_rainfields(
                self.signals,
                calc_data,
                self.cp,
                self.rain_grids,
                self.realtime_runs,
                self.last_time,
                log_run_id,
                self.results_id
            )
        except RainfieldsGenException:
            return

        print(f"[{log_run_id}] Rainfall calculation procedure ended.", flush=True)
