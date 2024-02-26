import datetime

import numpy as np
import lib.pycomlink.pycomlink.processing as pycmlp
import lib.pycomlink.pycomlink.spatial as pycmls
import xarray as xr
from PyQt6.QtCore import QRunnable, QObject, pyqtSignal

from procedures import temperature_correlation, temperature_compensation


class CalcSignals(QObject):
    overall_done_signal = pyqtSignal(dict)
    plots_done_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(dict)
    progress_signal = pyqtSignal(dict)


class Calculation(QRunnable):
    def __init__(self, influx_man, signals: CalcSignals, results_id: int, links: dict, selection: dict, cp: dict):

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

        # ////// DATA ACQUISITION \\\\\\
        try:
            if len(self.selection) < 1:
                raise ValueError('Empty selection container.')

            ips = []
            for link in self.selection:
                if link in self.links:
                    # TODO: add dynamic exception list of constant Tx power devices
                    # 1S10s have constant Tx power, so only one unit can be included in query
                    # otherwise, both ends needs to be included in query, due Tx power correction
                    if self.links[link].tech in "1s10":
                        if self.selection[link] == 1:
                            ips.append(self.links[link].ip_a)
                        elif self.selection[link] == 2:
                            ips.append(self.links[link].ip_b)
                        elif self.selection[link] == 3:
                            ips.append(self.links[link].ip_a)
                            ips.append(self.links[link].ip_b)
                    elif self.selection[link] == 0:
                        continue
                    else:
                        ips.append(self.links[link].ip_a)
                        ips.append(self.links[link].ip_b)

            self.signals.progress_signal.emit({'prg_val': 5})
            print(f"[{log_run_id}] Querying InfluxDB for selected microwave links data...", flush=True)

            # Realtime calculation is being done
            if self.cp['is_realtime']:
                print(f"[{log_run_id}] Realtime data procedure started.", flush=True)
                influx_data = self.influx_man.query_signal_mean_realtime(ips, self.cp['realtime_timewindow'],
                                                                         self.cp['step'])
            # In other case, notify we are doing historic calculation
            else:
                print(f"[{log_run_id}] Historic data procedure started.", flush=True)
                influx_data = self.influx_man.query_signal_mean(ips, self.cp['start'], self.cp['end'], self.cp['step'])

            diff = len(ips) - len(influx_data)

            self.signals.progress_signal.emit({'prg_val': 15})
            print(f"[{log_run_id}] Querying done. Got data of {len(influx_data)} units,"
                  f" of total {len(ips)} selected units.")

            missing_links = []
            if diff > 0:
                print(f"[{log_run_id}] {diff} units are not available in selected time window:")
                for ip in ips:
                    if ip not in influx_data:
                        for link in self.links:
                            if self.links[link].ip_a == ip:
                                print(f"[{log_run_id}] Link: {self.links[link].link_id}; "
                                      f"Tech: {self.links[link].tech}; SIDE A: {self.links[link].name_a}; "
                                      f"IP: {self.links[link].ip_a}")
                                missing_links.append(link)
                                break
                            elif self.links[link].ip_b == ip:
                                print(f"[{log_run_id}] Link: {self.links[link].link_id}; "
                                      f"Tech: {self.links[link].tech}; SIDE B: {self.links[link].name_b}; "
                                      f"IP: {self.links[link].ip_b}")
                                missing_links.append(link)
                                break

            self.signals.progress_signal.emit({'prg_val': 18})

        except BaseException as error:
            self.signals.error_signal.emit({"id": self.results_id})
            print(f"[{log_run_id}] ERROR: An unexpected error occurred during InfluxDB query: "
                  f"{type(error)} {error}.")
            print(f"[{log_run_id}] ERROR: Calculation thread terminated.")
            return

        # ////// PARSE INTO XARRAY, RESOLVE TX POWER ASSIGNMENT TO CORRECT CHANNEL \\\\\\

        calc_data = []
        link = 0

        try:

            link_count = len(self.selection)
            current_link = 0

            for link in self.selection:
                if self.selection[link] == 0:
                    continue

                tx_zeros_b = False
                tx_zeros_a = False

                is_a_in = self.links[link].ip_a in influx_data
                is_b_in = self.links[link].ip_b in influx_data

                # TODO: load from options list of constant Tx power devices
                is_constant_tx_power = self.links[link].tech in ("1s10",)
                # TODO: load from options list of bugged techs with missing Tx zeros in InfluxDB
                is_tx_power_bugged = self.links[link].tech in ("ceragon_ip_10",)

                # skip links, where data of one unit (or both) are not available
                # but constant Tx power devices are exceptions
                if not (is_a_in and is_b_in):
                    if not ((is_a_in != is_b_in) and is_constant_tx_power):
                        if link not in missing_links:
                            print(f"[{log_run_id}] INFO: Skipping link ID: {link}. "
                                  f"No unit data available.", flush=True)
                        # skip link
                        continue

                # skip links with missing Tx power data on the one of the units (unable to do Tx power correction)
                # Orcaves 1S10 and IP10Gs have constant Tx power, so it doesn't matter
                if is_constant_tx_power:
                    tx_zeros_b = True
                    tx_zeros_a = True
                elif ("tx_power" not in influx_data[self.links[link].ip_a]) or \
                        ("tx_power" not in influx_data[self.links[link].ip_b]):
                    # sadly, some devices of certain techs are badly exported from original source, and they are
                    # missing Tx zero values in InfluxDB, so this hack needs to be done
                    # (for other techs, there is no certainty, if original Tx value was zero in fact, or it's a NMS
                    # error and these values are missing, so it's better to skip that links)
                    if is_tx_power_bugged:
                        print(f"[{log_run_id}] INFO: Link ID: {link}. "
                              f"No Tx Power data available. Link technology \"{self.links[link].tech}\" is on "
                              f"exception list -> filling Tx data with zeros.", flush=True)
                        if "tx_power" not in influx_data[self.links[link].ip_b]:
                            tx_zeros_b = True
                        if "tx_power" not in influx_data[self.links[link].ip_a]:
                            tx_zeros_a = True
                    else:
                        print(f"[{log_run_id}] INFO: Skipping link ID: {link}. "
                              f"No Tx Power data available.", flush=True)
                        # skip link
                        continue

                # hack: since one dimensional freq var in xarray is crashing pycomlink, change one freq negligibly to
                # preserve an array of two frequencies (channel A, channel B)
                if self.links[link].freq_a == self.links[link].freq_b:
                    self.links[link].freq_a += 1

                link_channels = []

                # Side/unit A (channel B to A)
                if (self.selection[link] in (1, 3)) and (self.links[link].ip_a in influx_data):
                    if not tx_zeros_b:
                        if len(influx_data[self.links[link].ip_a]["rx_power"]) \
                                != len(influx_data[self.links[link].ip_b]["tx_power"]):
                            print(f"[{log_run_id}] WARNING: Skipping link ID: {link}. "
                                  f"Non-coherent Rx/Tx data on channel A(rx)_B(tx).", flush=True)
                            continue

                    channel_a = self._fill_channel_dataset(self.links[link], influx_data, self.links[link].ip_b,
                                                           self.links[link].ip_a, 'A(rx)_B(tx)',
                                                           self.links[link].freq_b, tx_zeros_b)
                    link_channels.append(channel_a)

                    # if including only this channel, create empty second channel and fill it with zeros (pycomlink
                    # functions require both channels included -> with this hack it's valid, but zeros have no effect)
                    if (self.selection[link] == 1) or not is_b_in:
                        channel_b = self._fill_channel_dataset(self.links[link], influx_data, self.links[link].ip_a,
                                                               self.links[link].ip_a, 'B(rx)_A(tx)',
                                                               self.links[link].freq_a, tx_zeros_b,
                                                               is_empty_channel=True)
                        link_channels.append(channel_b)

                # Side/unit B (channel A to B)
                if (self.selection[link] in (2, 3)) and (self.links[link].ip_b in influx_data):
                    if not tx_zeros_a:
                        if len(influx_data[self.links[link].ip_b]["rx_power"]) \
                                != len(influx_data[self.links[link].ip_a]["tx_power"]):
                            print(f"[{log_run_id}] WARNING: Skipping link ID: {link}. "
                                  f"Non-coherent Rx/Tx data on channel B(rx)_A(tx).", flush=True)
                            continue

                    channel_b = self._fill_channel_dataset(self.links[link], influx_data, self.links[link].ip_a,
                                                           self.links[link].ip_b, 'B(rx)_A(tx)',
                                                           self.links[link].freq_a, tx_zeros_a)
                    link_channels.append(channel_b)

                    # if including only this channel, create empty second channel and fill it with zeros (pycomlink
                    # functions require both channels included -> with this hack it's valid, but zeros have no effect)
                    if (self.selection[link] == 2) or not is_a_in:
                        channel_a = self._fill_channel_dataset(self.links[link], influx_data, self.links[link].ip_b,
                                                               self.links[link].ip_b, 'A(rx)_B(tx)',
                                                               self.links[link].freq_b, tx_zeros_b,
                                                               is_empty_channel=True)
                        link_channels.append(channel_a)

                calc_data.append(xr.concat(link_channels, dim="channel_id"))

                self.signals.progress_signal.emit({'prg_val': round((current_link / link_count) * 17) + 18})
                current_link += 1

            del influx_data

        except BaseException as error:
            self.signals.error_signal.emit({"id": self.results_id})
            print(f"[{log_run_id}] ERROR: An unexpected error occurred during data processing: "
                  f"{type(error)} {error}.")
            print(f"[{log_run_id}] ERROR: Last processed microwave link ID: {link}")
            print(f"[{log_run_id}] ERROR: Calculation thread terminated.")
            return

        # ////// RAINFALL CALCULATION \\\\\\

        try:

            print(f"[{log_run_id}] Smoothing signal data...")
            link_count = len(calc_data)
            current_link = 0
            count = 0

            # Creating array to remove high-correlation links (class correlation.py)
            links_to_delete = []

            # interpolate NaNs in input data; filter out outliers
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

                self.signals.progress_signal.emit({'prg_val': round((current_link / link_count) * 15) + 35})
                current_link += 1
                count += 1

                """
                # temperature_correlation  - remove links if the correlation exceeds the specified threshold
                # temperature_compensation - as correlation, but also replaces the original trsl with the corrected
                                             one, according to the created tempreture compensation algorithm
                """

                if self.cp['is_temp_filtered']:
                    print(f"[{log_run_id}] Remove-link procedure started.")
                    temperature_correlation.pearson_correlation(count, ips, current_link, links_to_delete, link,
                                                                self.cp['correlation_threshold'])

                if self.cp['is_temp_compensated']:
                    print(f"[{log_run_id}] Compensation algorithm procedure started.")
                    temperature_compensation.compensation(count, ips, current_link, link,
                                                          self.cp['correlation_threshold'])

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
                # determine wet periods
                link['wet'] = link.trsl.rolling(time=self.cp['rolling_values'], center=self.cp['is_window_centered'])\
                                  .std(skipna=False) > self.cp['wet_dry_deviation']

                # calculate ratio of wet periods
                link['wet_fraction'] = (link.wet == 1).sum() / (link.wet == 0).sum()

                # determine signal baseline
                link['baseline'] = pycmlp.baseline.baseline_constant(trsl=link.trsl, wet=link.wet,
                                                                     n_average_last_dry=self.cp['baseline_samples'])

                # calculate wet antenna attenuation
                link['waa'] = pycmlp.wet_antenna.waa_schleiss_2013(rsl=link.trsl, baseline=link.baseline, wet=link.wet,
                                                                   waa_max=self.cp['waa_schleiss_val'],
                                                                   delta_t=60 / ((60 / self.cp['step']) * 60),
                                                                   tau=self.cp['waa_schleiss_tau'])

                # calculate final rain attenuation
                link['A'] = link.trsl - link.baseline - link.waa

                # calculate rain intensity
                link['R'] = pycmlp.k_R_relation.calc_R_from_A(A=link.A, L_km=float(link.length),
                                                              f_GHz=link.frequency, pol=link.polarization)

                self.signals.progress_signal.emit({'prg_val': round((current_link / link_count) * 40) + 50})
                current_link += 1

        except BaseException as error:
            self.signals.error_signal.emit({"id": self.results_id})
            print(f"[{log_run_id}] ERROR: An unexpected error occurred during rain calculation: "
                  f"{type(error)} {error}.")
            print(
                f"[{log_run_id}] ERROR: Last processed microwave link dataset: {calc_data[current_link]}")
            print(f"[{log_run_id}] ERROR: Calculation thread terminated.")
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

    # noinspection PyMethodMayBeStatic

    def _fill_channel_dataset(self, current_link, flux_data, tx_ip, rx_ip, channel_id, freq,
                              tx_zeros: bool = False, is_empty_channel: bool = False) -> xr.Dataset:
        # get times from the Rx power array => since Rx unit should be always available, rx_ip can be used
        times = []
        for timestamp in flux_data[rx_ip]["rx_power"].keys():
            times.append(np.datetime64(timestamp).astype("datetime64[ns]"))

        # if creating empty channel dataset, fill Rx vars with zeros =>
        # => since Rx unit should be always available, rx_ip can be used
        if is_empty_channel:
            rsl = np.zeros((len(flux_data[rx_ip]["rx_power"]),), dtype=float)
            dummy = True
        else:
            rsl = [*flux_data[rx_ip]["rx_power"].values()]
            dummy = False

        # in case of Tx power zeros, we don't have data of Tx unit available in flux_data =>
        # => get array length from Rx array of rx_ip unit, since it should be always available
        if tx_zeros:
            tsl = np.zeros((len(flux_data[rx_ip]["rx_power"]),), dtype=float)
        else:
            tsl = [*flux_data[tx_ip]["tx_power"].values()]

        # if creating empty channel dataset, fill Temperature_Rx vars with zeros
        # since Rx unit should be always available, rx_ip can be used
        if is_empty_channel:
            temperature_rx = np.zeros((len(flux_data[rx_ip]["temperature"]),), dtype=float)
        else:
            temperature_rx = [*flux_data[rx_ip]["temperature"].values()]

        # in case of Tx power zeros, we don't have data of Tx unit available in flux_data =>
        # => get array length from Temperature array of rx_ip unit, since it should be always available
        if tx_zeros:
            temperature_tx = np.zeros((len(flux_data[rx_ip]["temperature"]),), dtype=float)
        else:
            temperature_tx = [*flux_data[tx_ip]["temperature"].values()]

        channel = xr.Dataset(
            data_vars={
                "tsl": ("time", tsl),
                "rsl": ("time", rsl),
                "temperature_rx": ("time", temperature_rx),
                "temperature_tx": ("time", temperature_tx),

            },
            coords={
                "time": times,
                "channel_id": channel_id,
                "cml_id": current_link.link_id,
                "site_a_latitude": current_link.latitude_a,
                "site_b_latitude": current_link.latitude_b,
                "site_a_longitude": current_link.longitude_a,
                "site_b_longitude": current_link.longitude_b,
                "frequency": freq / 1000,
                "polarization": current_link.polarization,
                "length": current_link.distance,
                "dummy_channel": dummy,
                "dummy_a_latitude": current_link.dummy_latitude_a,
                "dummy_b_latitude": current_link.dummy_latitude_b,
                "dummy_a_longitude": current_link.dummy_longitude_a,
                "dummy_b_longitude": current_link.dummy_longitude_b,
            },
        )
        return channel
