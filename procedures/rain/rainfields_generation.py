import traceback
from typing import Any

import numpy as np
import xarray as xr

import lib.pycomlink.pycomlink.spatial as pycmls

from handlers.logging_handler import logger
from procedures.calculation_signals import CalcSignals
from procedures.exceptions import RainfieldsGenException
from procedures.rain.links_segmentation import process_segments


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
        # *************************************************************************************************
        # ***** FIRST PART: Compute link segments with CML references (linear or intersection based) ******
        # *************************************************************************************************

        logger.info("[%s] Processing links segmentation...", log_run_id)
        process_segments(calc_data, cp["segment_size"], cp["is_intersection_enabled"], log_run_id)

        # ***********************************************************************
        # ***** SECOND PART: Calculate overall rainfall accumulation field ******
        # ***********************************************************************

        logger.info("[%s] Resampling rain values for rainfall overall map...", log_run_id)

        # combine CMLs into one dataset
        calc_data = xr.concat(calc_data, dim='cml_id')
        # calculate 1h means via resample
        rain_values_1h = calc_data.R.resample(time='1H', label='right').mean()
        # sum of all 1h means = total
        rain_values_total = rain_values_1h.mean(dim='channel_id').sum(dim='time')

        signals.progress_signal.emit({'prg_val': 93})

        logger.info("[%s] Interpolating spatial data for rainfall overall map...", log_run_id)

        interpolator = pycmls.interpolator.IdwKdtreeInterpolator(
            nnear=cp['idw_near'],
            p=cp['idw_power'],
            exclude_nan=True,
            max_distance=cp['idw_dist']
        )

        # calculate coordinate grids with defined area boundaries
        x_coords = np.arange(cp['X_MIN'], cp['X_MAX'], cp['interpol_res'])
        y_coords = np.arange(cp['Y_MIN'], cp['Y_MAX'], cp['interpol_res'])
        x_grid, y_grid = np.meshgrid(x_coords, y_coords)

        # flatten coordinates of the CMLs into 1D arrays
        lats_1dim = calc_data.lat_array.values.flatten()
        lats_1dim = lats_1dim[~np.isnan(lats_1dim)] # remove NaN values
        longs_1dim = calc_data.long_array.values.flatten()
        longs_1dim = longs_1dim[~np.isnan(longs_1dim)] # remove NaN values

        # get segments CML references
        seg_references = calc_data['cml_reference'].values.flatten() # flatten the 'cml_reference' to a 1D array
        seg_valid_mask = ~np.isnan(seg_references) # create a boolean mask where 'cml_reference' is not NaN
        seg_valid_refs = seg_references[seg_valid_mask].astype(int) # apply the mask to get only valid references

        # assign rain values to the segments according to their CML references
        rain_vals = rain_values_total.sel(cml_id=seg_valid_refs).values # select all corresponding rain values at once

        # interpolate the total rain field
        rain_grid = interpolator(
            x=longs_1dim,
            y=lats_1dim,
            z=np.asarray(rain_vals),
            xgrid=x_grid, ygrid=y_grid
        )

        signals.progress_signal.emit({'prg_val': 99})

        # get start and end timestamps
        data_start = calc_data.time.min().values
        data_end = calc_data.time.max().values

        # emit output
        signals.overall_done_signal.emit({
            "id": results_id,
            "start": data_start,
            "end": data_end,
            "calc_data": calc_data,
            "x_grid": x_grid,
            "y_grid": y_grid,
            "rain_grid": rain_grid,
            "is_it_all": cp['is_only_overall'],
        })

        # *******************************************************************
        # ***** THIRD PART: Calculate individual fields for animation *******
        # *******************************************************************

        # continue only if is it desired, else end
        if cp['is_only_overall']:
            return rain_grids, realtime_runs, last_time
        else:
            # progress bar goes from 0 again in second part
            signals.progress_signal.emit({'prg_val': 0})

            logger.info("[%s] Resampling data for rainfall animation maps...", log_run_id)

            # resample data to desired resolution, if needed
            if cp['output_step'] == 60:  # if case of one hour steps, use already existing resamples
                rain_values_steps = rain_values_1h
            elif cp['output_step'] > cp['step']:
                rain_values_steps = calc_data.R.resample(time=f'{cp["output_step"]}T', label='right').mean()
            elif cp['output_step'] == cp['step']:  # in case of same intervals, no resample needed
                rain_values_steps = calc_data.R
            else:
                raise ValueError("Invalid value of output_steps")

            signals.progress_signal.emit({'prg_val': 5})

            # calculate totals instead of intensities, if desired
            if cp['is_output_total']:
                # get calc ratio
                time_ratio = 60 / cp['output_step']  # 60 = 1 hour, since rain intensity is measured in mm/hour
                # overwrite values with totals per output step interval
                rain_values_steps = rain_values_steps / time_ratio

            signals.progress_signal.emit({'prg_val': 10})

            logger.info("[%s] Interpolating spatial data for rainfall animation maps...", log_run_id)

            # create counter for the number of old grids to delete, if in realtime mode (iteration > 1)
            grids_to_del = 0

            # interpolate each rain field
            for x in range(rain_values_steps.time.size):
                # check if the time is newer than the last time (to avoid duplicate calculations in realtime mode)
                if rain_values_steps.time[x].values > last_time:
                    # assign rain values for given time to the segments according to their CML references
                    rain_vals = rain_values_steps.sel(cml_id=seg_valid_refs).mean(dim='channel_id').isel(time=x).values

                    # interpolate the rain field
                    grid = interpolator(
                        x=longs_1dim,
                        y=lats_1dim,
                        z=np.asarray(rain_vals),
                        xgrid=x_grid, ygrid=y_grid
                    )

                    grid[grid < cp['min_rain_value']] = 0  # zeroing out small values below threshold
                    rain_grids.append(grid)
                    last_time = rain_values_steps.time[x].values # update last time

                    if realtime_runs > 1: # delete old grids if in realtime mode
                        grids_to_del += 1

                signals.progress_signal.emit({'prg_val': round((x / rain_values_steps.time.size) * 89) + 10})

            for x in range(grids_to_del):
                del rain_grids[x]

            # emit output
            signals.plots_done_signal.emit({
                "id": results_id,
                "calc_data": calc_data,
                "x_grid": x_grid,
                "y_grid": y_grid,
                "rain_grids": rain_grids,
            })

            # clean up
            del rain_values_steps
            del rain_values_total
            if rain_values_1h is not None:
                del rain_values_1h

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
