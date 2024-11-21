import traceback
from typing import Any

import numpy as np
import xarray as xr

import lib.pycomlink.pycomlink.spatial as pycmls

from handlers.logging_handler import logger
from procedures.calculation_signals import CalcSignals
from procedures.exceptions import RainfieldsGenException


def generate_rainfields(
        signals: CalcSignals,
        calc_data: list[xr.Dataset],
        cp: dict[str, Any],
        rain_grids: list[np.ndarray],
        realtime_runs: int,
        last_time: np.datetime64,
        log_run_id: str,
        results_id: int
) -> (list[np.ndarray], int, np.datetime64):
    try:
        # **********************************************************************
        # ***** FIRST PART: Calculate overall rainfall accumulation field ******
        # **********************************************************************

        logger.info("[%s] Resampling rain values for rainfall overall map...", log_run_id)

        # resample values to 1h means
        calc_data_1h = xr.concat(objs=[cml.R.resample(time='1H', label='right').mean() for cml in calc_data],
                                 dim='cml_id').to_dataset()

        signals.progress_signal.emit({'prg_val': 93})

        logger.info("[%s] Interpolating spatial data for rainfall overall map...", log_run_id)

        # TODO: use already created coords from external filter
        # if not cp['is_external_filter_enabled']:
        # central points of the links are considered in interpolation algorithms
        calc_data_1h['lat_center'] = (calc_data_1h.site_a_latitude + calc_data_1h.site_b_latitude) / 2
        calc_data_1h['lon_center'] = (calc_data_1h.site_a_longitude + calc_data_1h.site_b_longitude) / 2

        interpolator = pycmls.interpolator.IdwKdtreeInterpolator(nnear=cp['idw_near'],
                                                                 p=cp['idw_power'],
                                                                 exclude_nan=True,
                                                                 max_distance=cp['idw_dist'])

        # calculate coordinate grids with defined area boundaries
        x_coords = np.arange(cp['X_MIN'], cp['X_MAX'], cp['interpol_res'])
        y_coords = np.arange(cp['Y_MIN'], cp['Y_MAX'], cp['interpol_res'])
        x_grid, y_grid = np.meshgrid(x_coords, y_coords)

        rain_grid = interpolator(x=calc_data_1h.lon_center, y=calc_data_1h.lat_center,
                                 z=calc_data_1h.R.mean(dim='channel_id').sum(dim='time'),
                                 xgrid=x_grid, ygrid=y_grid)

        signals.progress_signal.emit({'prg_val': 99})

        # get start and end timestamps from lists of DataArrays
        data_start = calc_data[0].time.min()
        data_end = calc_data[0].time.max()

        for link in calc_data:
            times = link.time.values
            data_start = min(data_start, times.min())
            data_end = max(data_end, times.max())

        # emit output
        signals.overall_done_signal.emit({
            "id": results_id,
            "start": data_start,
            "end": data_end,
            "calc_data": calc_data_1h,
            "x_grid": x_grid,
            "y_grid": y_grid,
            "rain_grid": rain_grid,
            "is_it_all": cp['is_only_overall'],
        })

        # *******************************************************************
        # ***** SECOND PART: Calculate individual fields for animation ******
        # *******************************************************************

        # continue only if is it desired, else end
        if not cp['is_only_overall']:

            logger.info("[%s] Resampling data for rainfall animation maps...", log_run_id)

            # resample data to desired resolution, if needed
            if cp['output_step'] == 60:  # if case of one hour steps, use already existing resamples
                calc_data_steps = calc_data_1h
            elif cp['output_step'] > cp['step']:
                os = cp['output_step']
                calc_data_steps = xr.concat(
                    objs=[cml.R.resample(time=f'{os}T', label='right').mean() for cml in calc_data], dim='cml_id'
                ).to_dataset()
            elif cp['output_step'] == cp['step']:  # in case of same intervals, no resample needed
                calc_data_steps = xr.concat(calc_data, dim='cml_id')
            else:
                raise ValueError("Invalid value of output_steps")

            # progress bar goes from 0 in second part
            signals.progress_signal.emit({'prg_val': 5})

            del calc_data

            # calculate totals instead of intensities, if desired
            if cp['is_output_total']:
                # get calc ratio
                time_ratio = 60 / cp['output_step']  # 60 = 1 hour, since rain intensity is measured in mm/hour
                # overwrite values with totals per output step interval
                calc_data_steps['R'] = calc_data_steps.R / time_ratio

            signals.progress_signal.emit({'prg_val': 10})

            logger.info("[%s] Interpolating spatial data for rainfall animation maps...", log_run_id)

            # if output step is 60, it's already done
            if cp['output_step'] != 60:
                # central points of the links are considered in interpolation algorithms
                calc_data_steps['lat_center'] = \
                    (calc_data_steps.site_a_latitude + calc_data_steps.site_b_latitude) / 2
                calc_data_steps['lon_center'] = \
                    (calc_data_steps.site_a_longitude + calc_data_steps.site_b_longitude) / 2

            grids_to_del = 0
            # interpolate each frame
            for x in range(calc_data_steps.time.size):
                if calc_data_steps.time[x].values > last_time:
                    grid = interpolator(x=calc_data_steps.lon_center, y=calc_data_steps.lat_center,
                                        z=calc_data_steps.R.mean(dim='channel_id').isel(time=x),
                                        xgrid=x_grid, ygrid=y_grid)
                    grid[grid < cp['min_rain_value']] = 0  # zeroing out small values below threshold
                    rain_grids.append(grid)
                    last_time = calc_data_steps.time[x].values

                    if realtime_runs > 1:
                        grids_to_del += 1

                signals.progress_signal.emit({'prg_val': round((x / calc_data_steps.time.size) * 89) + 10})

            for x in range(grids_to_del):
                del rain_grids[x]

            # emit output
            signals.plots_done_signal.emit({
                "id": results_id,
                "calc_data": calc_data_steps,
                "x_grid": x_grid,
                "y_grid": y_grid,
                "rain_grids": rain_grids,
            })

            del calc_data_steps
            if calc_data_1h is not None:
                del calc_data_1h

            return rain_grids, realtime_runs, last_time

    except BaseException as error:
        signals.error_signal.emit({"id": results_id})

        logger.error(
            "[%s] An error occurred during rainfall fields generation: %s %s.\n"
            "Calculation thread terminated.",
            log_run_id, type(error), error
        )

        traceback.print_exc()

        raise RainfieldsGenException("Error occurred during rainfall fields generation processing")
