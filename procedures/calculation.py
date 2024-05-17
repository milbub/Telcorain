import datetime

import numpy as np
import xarray as xr
from PyQt6.QtCore import QRunnable

import lib.pycomlink.pycomlink.spatial as pycmls

from database.influx_manager import InfluxManager
from procedures.calculation_signals import CalcSignals
from procedures.exceptions import ProcessingException, RaincalcException
from procedures.data import data_loading, data_preprocessing
from procedures.rain import rain_calculation


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

        # ////// RESAMPLE AND SPATIAL INTERPOLATION \\\\\\
        try:

            # ***** FIRST PART: Calculate overall rainfall total map ******
            print(f"[{log_run_id}] Resampling rain values for rainfall overall map...")

            # resample values to 1h means
            calc_data_1h = xr.concat(objs=[cml.R.resample(time='1h', label='right').mean() for cml in calc_data],
                                     dim='cml_id').to_dataset()

            self.signals.progress_signal.emit({'prg_val': 93})

            print(f"[{log_run_id}] Interpolating spatial data for rainfall overall map...")

            # TODO: use already created coords from external filter
            # if not self.cp['is_external_filter_enabled']:
            # central points of the links are considered in interpolation algorithms
            calc_data_1h['lat_center'] = (calc_data_1h.site_a_latitude + calc_data_1h.site_b_latitude) / 2
            calc_data_1h['lon_center'] = (calc_data_1h.site_a_longitude + calc_data_1h.site_b_longitude) / 2

            interpolator = pycmls.interpolator.IdwKdtreeInterpolator(nnear=self.cp['idw_near'], p=self.cp['idw_power'],
                                                                     exclude_nan=True,
                                                                     max_distance=self.cp['idw_dist'])

            # calculate coordinate grids with defined area boundaries
            x_coords = np.arange(self.cp['X_MIN'], self.cp['X_MAX'], self.cp['interpol_res'])
            y_coords = np.arange(self.cp['Y_MIN'], self.cp['Y_MAX'], self.cp['interpol_res'])
            x_grid, y_grid = np.meshgrid(x_coords, y_coords)

            rain_grid = interpolator(x=calc_data_1h.lon_center, y=calc_data_1h.lat_center,
                                     z=calc_data_1h.R.mean(dim='channel_id').sum(dim='time'),
                                     xgrid=x_grid, ygrid=y_grid)

            self.signals.progress_signal.emit({'prg_val': 99})

            # get start and end timestamps from lists of DataArrays
            data_start = calc_data[0].time.min()
            data_end = calc_data[0].time.max()

            for link in calc_data:
                times = link.time.values
                data_start = min(data_start, times.min())
                data_end = max(data_end, times.max())

            # emit output
            self.signals.overall_done_signal.emit({
                "id": self.results_id,
                "start": data_start,
                "end": data_end,
                "calc_data": calc_data_1h,
                "x_grid": x_grid,
                "y_grid": y_grid,
                "rain_grid": rain_grid,
                "is_it_all": self.cp['is_only_overall'],
            })

            # ***** SECOND PART: Calculate individual maps for animation ******

            # continue only if is it desired, else end
            if not self.cp['is_only_overall']:

                print(f"[{log_run_id}] Resampling data for rainfall animation maps...")

                # resample data to desired resolution, if needed
                if self.cp['output_step'] == 60:  # if case of one hour steps, use already existing resamples
                    calc_data_steps = calc_data_1h
                elif self.cp['output_step'] > self.cp['step']:
                    os = self.cp['output_step']
                    calc_data_steps = xr.concat(
                        objs=[cml.R.resample(time=f'{os}m', label='right').mean() for cml in calc_data], dim='cml_id'
                    ).to_dataset()
                elif self.cp['output_step'] == self.cp['step']:  # in case of same intervals, no resample needed
                    calc_data_steps = xr.concat(calc_data, dim='cml_id')
                else:
                    raise ValueError("Invalid value of output_steps")

                # progress bar goes from 0 in second part
                self.signals.progress_signal.emit({'prg_val': 5})

                del calc_data

                # calculate totals instead of intensities, if desired
                if self.cp['is_output_total']:
                    # get calc ratio
                    time_ratio = 60 / self.cp['output_step']  # 60 = 1 hour, since rain intensity is measured in mm/hour
                    # overwrite values with totals per output step interval
                    calc_data_steps['R'] = calc_data_steps.R / time_ratio

                self.signals.progress_signal.emit({'prg_val': 10})

                print(f"[{log_run_id}] Interpolating spatial data for rainfall animation maps...")

                # if output step is 60, it's already done
                if self.cp['output_step'] != 60:
                    # central points of the links are considered in interpolation algorithms
                    calc_data_steps['lat_center'] = \
                        (calc_data_steps.site_a_latitude + calc_data_steps.site_b_latitude) / 2
                    calc_data_steps['lon_center'] = \
                        (calc_data_steps.site_a_longitude + calc_data_steps.site_b_longitude) / 2

                grids_to_del = 0
                # interpolate each frame
                for x in range(calc_data_steps.time.size):
                    if calc_data_steps.time[x].values > self.last_time:
                        grid = interpolator(x=calc_data_steps.lon_center, y=calc_data_steps.lat_center,
                                            z=calc_data_steps.R.mean(dim='channel_id').isel(time=x),
                                            xgrid=x_grid, ygrid=y_grid)
                        grid[grid < self.cp['min_rain_value']] = 0  # zeroing out small values below threshold
                        self.rain_grids.append(grid)
                        self.last_time = calc_data_steps.time[x].values

                        if self.realtime_runs > 1:
                            grids_to_del += 1

                    self.signals.progress_signal.emit({'prg_val': round((x / calc_data_steps.time.size) * 89) + 10})

                for x in range(grids_to_del):
                    del self.rain_grids[x]

                # emit output
                self.signals.plots_done_signal.emit({
                    "id": self.results_id,
                    "calc_data": calc_data_steps,
                    "x_grid": x_grid,
                    "y_grid": y_grid,
                    "rain_grids": self.rain_grids,
                })

                del calc_data_steps
                if calc_data_1h is not None:
                    del calc_data_1h

        except BaseException as error:
            self.signals.error_signal.emit({"id": self.results_id})
            print(f"[{log_run_id}] ERROR: An unexpected error occurred during spatial interpolation: "
                  f"{type(error)} {error}.")
            print(f"[{log_run_id}] ERROR: Calculation thread terminated.")
            return

        print(f"[{log_run_id}] Rainfall calculation procedure ended.", flush=True)
