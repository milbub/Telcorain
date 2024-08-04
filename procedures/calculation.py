import datetime
from typing import Union

import numpy as np
import xarray as xr
from PyQt6.QtCore import QRunnable

from database.influx_manager import InfluxManager
from database.models.mwlink import MwLink
from handlers.logging_handler import logger
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
            links: dict[int, MwLink],
            selection: dict[int, int],
            cp: dict
    ):

        QRunnable.__init__(self)
        self.influx_man: InfluxManager = influx_man
        self.signals: CalcSignals = signals
        self.results_id: int = results_id
        self.links: dict[int, MwLink] = links
        self.selection: dict[int, int] = selection

        # calculation parameters dictionary
        self.cp = cp

        # run counter in case of realtime calculation
        self.realtime_runs: int = 0

        # store raingrids for possible next iteration (no need for repeated generating in realtime)
        self.rain_grids: list[np.ndarray] = []
        self.last_time: np.datetime64 = np.datetime64(datetime.datetime.min)

    def run(self):
        self.realtime_runs += 1
        if self.cp['is_realtime']:
            log_run_id = "CALC ID: " + str(self.results_id) + ", RUN: " + str(self.realtime_runs)
        else:
            log_run_id = "CALC ID: " + str(self.results_id)

        logger.info("[%s] Rainfall calculation procedure started.", log_run_id)

        try:
            # Gather data from InfluxDB
            influx_data: dict[str, Union[dict[str, dict[datetime, float]], str]]
            missing_links: list[int]
            ips: list[str]
            influx_data, missing_links, ips = data_loading.load_data_from_influxdb(
                influx_man=self.influx_man,
                signals=self.signals,
                cp=self.cp,
                selected_links=self.selection,
                links=self.links,
                log_run_id=log_run_id,
                results_id=self.results_id
            )

            # Merge influx data with metadata into datasets, resolve Tx power assignment to correct channel
            calc_data: list[xr.Dataset] = data_preprocessing.convert_to_link_datasets(
                signals=self.signals,
                selected_links=self.selection,
                links=self.links,
                influx_data=influx_data,
                missing_links=missing_links,
                log_run_id=log_run_id,
                results_id=self.results_id
            )
            del influx_data
        except ProcessingException:
            return

        try:
            # Obtain rain rates and store them in the calc_data
            calc_data: list[xr.Dataset] = rain_calculation.get_rain_rates(
                signals=self.signals,
                calc_data=calc_data,
                cp=self.cp,
                ips=ips,
                log_run_id=log_run_id,
                results_id=self.results_id
            )
        except RaincalcException:
            return

        try:
            # Generate rainfields (resample rain rates and interpolate them to a grid)
            self.rain_grids, self.realtime_runs, self.last_time = rainfields_generation.generate_rainfields(
                signals=self.signals,
                calc_data=calc_data,
                cp=self.cp,
                rain_grids=self.rain_grids,
                realtime_runs=self.realtime_runs,
                last_time=self.last_time,
                log_run_id=log_run_id,
                results_id=self.results_id
            )
        except RainfieldsGenException:
            return

        logger.info("[%s] Rainfall calculation procedure ended.", log_run_id)
