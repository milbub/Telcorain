from typing import Any

import numpy as np
from xarray import Dataset

import lib.pycomlink.pycomlink.processing as pycmlp
from lib.pycomlink.pycomlink.processing.wet_dry import cnn
from lib.pycomlink.pycomlink.processing.wet_dry.cnn import CNN_OUTPUT_LEFT_NANS_LENGTH

from procedures.calculation_signals import CalcSignals
from procedures.exceptions import RaincalcException
from procedures.rain import temperature_compensation, temperature_correlation
from procedures.utils.external_filter import determine_wet


def get_rain_rates(signals: CalcSignals, calc_data: list[Dataset], cp: dict[str, Any], ips: list[str], log_run_id: str, results_id: int) -> list[Dataset]:
    current_link = 0

    try:
        print(f"[{log_run_id}] Smoothing signal data...")
        link_count = len(calc_data)
        count = 0

        # Creating array to remove high-correlation links (class correlation.py)
        links_to_delete = []

        for link in calc_data:
            # TODO: load upper tx power from options (here it's 40 dBm)
            link['tsl'] = link.tsl.astype(float).where(link.tsl < 40.0)
            link['tsl'] = link.tsl.astype(float).interpolate_na(dim='time', method='nearest', max_gap=None)

            # TODO: load bottom rx power from options (here it's -70 dBm)
            link['rsl'] = link.rsl.astype(float).where(link.rsl != 0.0).where(link.rsl > -70.0)
            link['rsl'] = link.rsl.astype(float).interpolate_na(dim='time', method='nearest', max_gap=None)

            link['trsl'] = link.tsl - link.rsl

            link['temperature_rx'] = link.temperature_rx.astype(float).interpolate_na(dim='time',
                                                                                      method='linear',
                                                                                      max_gap=None)

            link['temperature_tx'] = link.temperature_tx.astype(float).interpolate_na(dim='time',
                                                                                      method='linear',
                                                                                      max_gap=None)

            signals.progress_signal.emit({'prg_val': round((current_link / link_count) * 15) + 35})
            current_link += 1
            count += 1

            """
            # temperature_correlation  - remove links if the correlation exceeds the specified threshold
            # temperature_compensation - as correlation, but also replaces the original trsl with the corrected
                                         one, according to the created tempreture compensation algorithm
            """

            if cp['is_temp_filtered']:
                print(f"[{log_run_id}] Remove-link procedure started.")
                temperature_correlation.pearson_correlation(count, ips, current_link, links_to_delete, link,
                                                            cp['correlation_threshold'])

            if cp['is_temp_compensated']:
                print(f"[{log_run_id}] Compensation algorithm procedure started.")
                temperature_compensation.compensation(count, ips, current_link, link,
                                                      cp['correlation_threshold'])

            """
            'current_link += 1' serves to accurately list the 'count' and ip address of CML unit
             when the 'temperature_compensation.py' or 'temperature_correlation.py' is called
            """
            current_link += 1

        # Run the removal of high correlation links (class correlation.py)
        for link in links_to_delete:
            calc_data.remove(link)

        # process each link -> get intensity R value for each link:
        print(f"[{log_run_id}] Computing rain values...")
        current_link = 0

        for link in calc_data:
            if cp['is_cnn_enabled']:
                # determine wet periods using CNN
                link['wet'] = (('time',), np.zeros([link.time.size]))

                cnn_out = cnn.cnn_wet_dry(
                    trsl_channel_1=link.isel(channel_id=0).trsl.values,
                    trsl_channel_2=link.isel(channel_id=1).trsl.values,
                    threshold=0.82,
                    batch_size=128
                )

                link['wet'] = (('time',), np.where(np.isnan(cnn_out), link['wet'], cnn_out))
            else:
                # determine wet periods using rolling standard deviation
                link['wet'] = link.trsl.rolling(
                    time=cp['rolling_values'],
                    center=cp['is_window_centered']
                ).std(skipna=False) > cp['wet_dry_deviation']

        if cp['is_cnn_enabled']:
            # remove first CNN_OUTPUT_LEFT_NANS_LENGTH time values from dataset since they are NaNs
            calc_data = [link.isel(time=slice(CNN_OUTPUT_LEFT_NANS_LENGTH, None)) for link in calc_data]

        if cp['is_external_filter_enabled']:
            efp = cp['external_filter_params']
            for link in calc_data:
                # central points of the links are sent into external filter
                link['lat_center'] = (link.site_a_latitude + link.site_b_latitude) / 2
                link['lon_center'] = (link.site_a_longitude + link.site_b_longitude) / 2

                for t in range(len(link.time)):
                    time = link.time[t].values
                    external_wet = determine_wet(
                        time,
                        link.lon_center,
                        link.lat_center,
                        efp['radius'] + link.length / 2,
                        efp['pixel_threshold'],
                        efp['IMG_X_MIN'],
                        efp['IMG_X_MAX'],
                        efp['IMG_Y_MIN'],
                        efp['IMG_Y_MAX'],
                        efp['url'],
                        efp['default_return'],
                        not cp['is_realtime']
                    )
                    internal_wet = link.wet[t].values
                    link.wet[t] = external_wet and internal_wet
                    print(f"[{log_run_id}] [EXTERNAL FILTER]: cml: {link.cml_id.values} time: {time} "
                          f"EXWET: {external_wet} INTWET: {internal_wet} = {link.wet[t].values}")

        for link in calc_data:
            # calculate ratio of wet periods
            link['wet_fraction'] = (link.wet == 1).sum() / len(link.time)

            # determine signal baseline
            link['baseline'] = pycmlp.baseline.baseline_constant(trsl=link.trsl, wet=link.wet,
                                                                 n_average_last_dry=cp['baseline_samples'])

            # calculate wet antenna attenuation
            link['waa'] = pycmlp.wet_antenna.waa_schleiss_2013(rsl=link.trsl, baseline=link.baseline, wet=link.wet,
                                                               waa_max=cp['waa_schleiss_val'],
                                                               delta_t=60 / ((60 / cp['step']) * 60),
                                                               tau=cp['waa_schleiss_tau'])

            # calculate final rain attenuation
            link['A'] = link.trsl - link.baseline - link.waa

            # calculate rain intensity
            link['R'] = pycmlp.k_R_relation.calc_R_from_A(A=link.A, L_km=float(link.length),
                                                          f_GHz=link.frequency, pol=link.polarization)

            signals.progress_signal.emit({'prg_val': round((current_link / link_count) * 40) + 50})
            current_link += 1

        return calc_data

    except BaseException as error:
        signals.error_signal.emit({"id": results_id})

        print(f"[{log_run_id}] ERROR: An unexpected error occurred during rain calculation: {type(error)} {error}.")
        print(f"[{log_run_id}] ERROR: Last processed microwave link dataset: {calc_data[current_link]}")
        print(f"[{log_run_id}] ERROR: Calculation thread terminated.")

        raise RaincalcException("Error occurred during rainfall calculation processing")