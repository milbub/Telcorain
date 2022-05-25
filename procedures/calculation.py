import numpy as np
import pycomlink as pycml
import xarray as xr
from PyQt6.QtCore import QRunnable, QObject, QDateTime, pyqtSignal

import input.influx_manager as influx


class CalcSignals(QObject):
    overall_done_signal = pyqtSignal(dict)
    plots_done_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(dict)
    progress_signal = pyqtSignal(dict)


class Calculation(QRunnable):
    # TODO: load from options
    # rendered area borders
    X_MIN = 14.21646819
    X_MAX = 14.70604375
    Y_MIN = 49.91505682
    Y_MAX = 50.22841327

    def __init__(self, signals: CalcSignals, results_id: int, links: dict, selection: dict, start: QDateTime,
                 end: QDateTime, interval: int, rolling_vals: int, output_step: int, is_only_overall: bool,
                 is_output_total: bool, wet_dry_deviation: float, baseline_samples: int, interpol_res, idw_pow,
                 idw_near, idw_dist, schleiss_val, schleiss_tau):
        QRunnable.__init__(self)
        self.sig = signals
        self.results_id = results_id
        self.links = links
        self.selection = selection
        self.start = start
        self.end = end
        self.interval = interval
        self.rolling_vals = rolling_vals
        self.output_step = output_step
        self.is_only_overall = is_only_overall
        self.is_output_total = is_output_total
        self.wet_dry_deviation = wet_dry_deviation
        self.baseline_samples = baseline_samples
        self.interpol_res = interpol_res
        self.idw_pow = idw_pow
        self.idw_near = idw_near
        self.idw_dist = idw_dist
        self.schleiss_val = schleiss_val
        self.schleiss_tau = schleiss_tau

    def run(self):
        print(f"[CALC ID: {self.results_id}] Rainfall calculation procedure started.", flush=True)

        # ////// DATA ACQUISITION \\\\\\

        try:
            if len(self.selection) < 1:
                raise ValueError('Empty selection container.')

            man = influx.InfluxManager()
            ips = []
            for link in self.selection:
                if link in self.links:
                    # TODO: add dynamic exception list of constant Tx power devices
                    # 1S10s and IP20Gs have constant Tx power, so only one unit can be included in query
                    # otherwise, both ends needs to be included in query, due Tx power correction
                    if self.links[link].tech in ("1s10", "ip20G"):
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

            self.sig.progress_signal.emit({'prg_val': 5})
            print(f"[CALC ID: {self.results_id}] Querying InfluxDB for selected microwave links data...", flush=True)

            influx_data = man.query_signal_mean(ips, self.start, self.end, self.interval)

            diff = len(ips) - len(influx_data)

            self.sig.progress_signal.emit({'prg_val': 15})
            print(f"[CALC ID: {self.results_id}] Querying done. Got data of {len(influx_data)} units,"
                  f" of total {len(ips)} selected units.")

            missing_links = []
            if diff > 0:
                print(f"[CALC ID: {self.results_id}] {diff} units are not available in selected time window:")
                for ip in ips:
                    if ip not in influx_data:
                        for link in self.links:
                            if self.links[link].ip_a == ip:
                                print(f"[CALC ID: {self.results_id}] Link: {self.links[link].link_id}; "
                                      f"Tech: {self.links[link].tech}; SIDE A: {self.links[link].name_a}; "
                                      f"IP: {self.links[link].ip_a}")
                                missing_links.append(link)
                                break
                            elif self.links[link].ip_b == ip:
                                print(f"[CALC ID: {self.results_id}] Link: {self.links[link].link_id}; "
                                      f"Tech: {self.links[link].tech}; SIDE B: {self.links[link].name_b}; "
                                      f"IP: {self.links[link].ip_b}")
                                missing_links.append(link)
                                break

            self.sig.progress_signal.emit({'prg_val': 18})

        except BaseException as error:
            self.sig.error_signal.emit({"id": self.results_id})
            print(f"[CALC ID: {self.results_id}] ERROR: An unexpected error occurred during InfluxDB query: "
                  f"{type(error)}.")
            print(f"[CALC ID: {self.results_id}] ERROR: Calculation thread terminated.")
            return

        # ////// PARSE INTO XARRAY, RESOLVE TX POWER ASSIGNMENT TO CORRECT CHANNEL \\\\\\

        calc_data = []
        link = 0

        try:

            link_count = len(self.selection)
            curr_link = 0

            for link in self.selection:
                if self.selection[link] == 0:
                    continue

                tx_zeros_b = False
                tx_zeros_a = False

                is_a_in = self.links[link].ip_a in influx_data
                is_b_in = self.links[link].ip_b in influx_data

                # TODO: load from options list of constant Tx power devices
                is_constant_tx_power = self.links[link].tech in ("1s10", "ip20G", )
                # TODO: load from options list of bugged techs with missing Tx zeros in InfluxDB
                is_tx_power_bugged = self.links[link].tech in ("ip10", )

                # skip links, where data of one unit (or both) are not available
                # but constant Tx power devices are exceptions
                if not (is_a_in and is_b_in):
                    if not ((is_a_in != is_b_in) and is_constant_tx_power):
                        if link not in missing_links:
                            print(f"[CALC ID: {self.results_id}] INFO: Skipping link ID: {link}. "
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
                        print(f"[CALC ID: {self.results_id}] INFO: Link ID: {link}. "
                              f"No Tx Power data available. Link technology \"{self.links[link].tech}\" is on "
                              f"exception list -> filling Tx data with zeros.", flush=True)
                        if "tx_power" not in influx_data[self.links[link].ip_b]:
                            tx_zeros_b = True
                        if "tx_power" not in influx_data[self.links[link].ip_a]:
                            tx_zeros_a = True
                    else:
                        print(f"[CALC ID: {self.results_id}] INFO: Skipping link ID: {link}. "
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
                        if len(influx_data[self.links[link].ip_a]["rx_power"])\
                                != len(influx_data[self.links[link].ip_b]["tx_power"]):
                            print(f"[CALC ID: {self.results_id}] WARNING: Skipping link ID: {link}. "
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
                                                               self.links[link].freq_a, tx_zeros_b, rx_zeros=True)
                        link_channels.append(channel_b)

                # Side/unit B (channel A to B)
                if (self.selection[link] in (2, 3)) and (self.links[link].ip_b in influx_data):
                    if not tx_zeros_a:
                        if len(influx_data[self.links[link].ip_b]["rx_power"])\
                                != len(influx_data[self.links[link].ip_a]["tx_power"]):
                            print(f"[CALC ID: {self.results_id}] WARNING: Skipping link ID: {link}. "
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
                                                               self.links[link].freq_b, tx_zeros_b, rx_zeros=True)
                        link_channels.append(channel_a)

                calc_data.append(xr.concat(link_channels, dim="channel_id"))

                self.sig.progress_signal.emit({'prg_val': round((curr_link / link_count) * 17) + 18})
                curr_link += 1

        except BaseException as error:
            self.sig.error_signal.emit({"id": self.results_id})
            print(f"[CALC ID: {self.results_id}] ERROR: An unexpected error occurred during data processing: "
                  f"{type(error)} {error}.")
            print(f"[CALC ID: {self.results_id}] ERROR: Last processed microwave link ID: {link}")
            print(f"[CALC ID: {self.results_id}] ERROR: Calculation thread terminated.")
            return

        # ////// RAINFALL CALCULATION \\\\\\

        try:

            print(f"[CALC ID: {self.results_id}] Smoothing signal data...")
            link_count = len(calc_data)
            curr_link = 0

            # interpolate NaNs in input data and filter out nonsenses out of limits
            for link in calc_data:
                # TODO: load upper tx power from options (here it's 99 dBm)
                link['tsl'] = link.tsl.astype(float).where(link.tsl < 99.0)
                link['tsl'] = link.tsl.astype(float).interpolate_na(dim='time', method='linear', max_gap='5min')
                # TODO: load bottom rx power from options (here it's -80 dBm)
                link['rsl'] = link.rsl.astype(float).where(link.rsl != 0.0).where(link.rsl > -80.0)
                link['rsl'] = link.rsl.astype(float).interpolate_na(dim='time', method='linear', max_gap='5min')

                link['trsl'] = link.tsl - link.rsl
                link['trsl'] = link.trsl.astype(float).interpolate_na(dim='time', method='nearest', max_gap='5min')
                link['trsl'] = link.trsl.astype(float).fillna(0.0)

                self.sig.progress_signal.emit({'prg_val': round((curr_link / link_count) * 15) + 35})
                curr_link += 1

            # process each link -> get intensity R value for each link:
            print(f"[CALC ID: {self.results_id}] Computing rain values...")
            curr_link = 0

            for link in calc_data:

                # determine wet periods
                link['wet'] = link.trsl.rolling(time=self.rolling_vals, center=True).std(skipna=False) > \
                              self.wet_dry_deviation

                # calculate ratio of wet periods
                link['wet_fraction'] = (link.wet == 1).sum() / (link.wet == 0).sum()

                # determine signal baseline
                link['baseline'] = pycml.processing.baseline.baseline_constant(trsl=link.trsl, wet=link.wet,
                                                                               n_average_last_dry=self.baseline_samples)

                # calculate wet antenna attenuation
                link['waa'] = pycml.processing.wet_antenna.waa_schleiss_2013(rsl=link.trsl, baseline=link.baseline,
                                                                             wet=link.wet, waa_max=self.schleiss_val,
                                                                             delta_t=60 / ((60 / self.interval) * 60),
                                                                             tau=self.schleiss_tau)

                # calculate final rain attenuation
                link['A'] = link.trsl - link.baseline - link.waa

                # calculate rain intensity
                link['R'] = pycml.processing.k_R_relation.calc_R_from_A(A=link.A, L_km=float(link.length),
                                                                        f_GHz=link.frequency, pol=link.polarization)

                self.sig.progress_signal.emit({'prg_val': round((curr_link / link_count) * 40) + 50})
                curr_link += 1

        except BaseException as error:
            self.sig.error_signal.emit({"id": self.results_id})
            print(f"[CALC ID: {self.results_id}] ERROR: An unexpected error occurred during rain calculation: "
                  f"{type(error)} {error}.")
            print(f"[CALC ID: {self.results_id}] ERROR: Last processed microwave link dataset: {calc_data[curr_link]}")
            print(f"[CALC ID: {self.results_id}] ERROR: Calculation thread terminated.")
            return

        # ////// RESAMPLE AND SPATIAL INTERPOLATION \\\\\\

        try:

            # ***** FIRST PART: Calculate overall rainfall total map ******
            print(f"[CALC ID: {self.results_id}] Resampling rain values for rainfall overall map...")

            # resample values to 1h means
            calc_data_1h = xr.concat(objs=[cml.R.resample(time='1h', label='right').mean() for cml in calc_data],
                                     dim='cml_id').to_dataset()

            self.sig.progress_signal.emit({'prg_val': 93})

            print(f"[CALC ID: {self.results_id}] Interpolating spatial data for rainfall overall map...")

            # central points of the links are considered in interpolation algorithms
            calc_data_1h['lat_center'] = (calc_data_1h.site_a_latitude + calc_data_1h.site_b_latitude) / 2
            calc_data_1h['lon_center'] = (calc_data_1h.site_a_longitude + calc_data_1h.site_b_longitude) / 2

            interpolator = pycml.spatial.interpolator.IdwKdtreeInterpolator(nnear=self.idw_near, p=self.idw_pow,
                                                                            exclude_nan=True,
                                                                            max_distance=self.idw_dist)

            # calculate coordinate grids with defined area boundaries
            x_coords = np.arange(self.X_MIN - self.interpol_res, self.X_MAX + self.interpol_res, self.interpol_res)
            y_coords = np.arange(self.Y_MIN - self.interpol_res, self.Y_MAX + self.interpol_res, self.interpol_res)
            x_grid, y_grid = np.meshgrid(x_coords, y_coords)

            rain_grid = interpolator(x=calc_data_1h.lon_center, y=calc_data_1h.lat_center,
                                     z=calc_data_1h.R.mean(dim='channel_id').sum(dim='time'),
                                     xgrid=x_grid, ygrid=y_grid)

            self.sig.progress_signal.emit({'prg_val': 99})

            # emit output
            self.sig.overall_done_signal.emit({
                "id": self.results_id,
                "link_data": calc_data_1h,
                "x_grid": x_grid,
                "y_grid": y_grid,
                "rain_grid": rain_grid,
                "is_it_all": self.is_only_overall,
            })

            # ***** SECOND PART: Calculate individual maps for animation ******

            # continue only if is it desired, else end
            if not self.is_only_overall:

                print(f"[CALC ID: {self.results_id}] Resampling data for rainfall animation maps...")

                # resample data to desired resolution, if needed
                if self.output_step == 60:   # if case of one hour steps, use already existing resamples
                    calc_data_steps = calc_data_1h
                elif self.output_step > self.interval:
                    calc_data_steps = xr.concat(
                        objs=[cml.R.resample(time=f'{self.output_step}m', label='right').mean() for cml in calc_data],
                        dim='cml_id').to_dataset()
                elif self.output_step == self.interval:   # in case of same intervals, no resample needed
                    calc_data_steps = xr.concat(calc_data, dim='cml_id')
                else:
                    raise ValueError("Invalid value of output_steps")

                # progress bar goes from 0 in second part
                self.sig.progress_signal.emit({'prg_val': 5})

                # calculate totals instead of intensities, if desired
                if self.is_output_total:
                    # get calc ratio
                    time_ratio = 60 / self.output_step   # 60 = 1 hour, since rain intensity is measured in mm/hour
                    # overwrite values with totals per output step interval
                    calc_data_steps['R'] = calc_data_steps.R / time_ratio

                self.sig.progress_signal.emit({'prg_val': 10})

                print(f"[CALC ID: {self.results_id}] Interpolating spatial data for rainfall animation maps...")

                # if output step is 60, it's already done
                if self.output_step != 60:
                    # central points of the links are considered in interpolation algorithms
                    calc_data_steps['lat_center'] = \
                        (calc_data_steps.site_a_latitude + calc_data_steps.site_b_latitude) / 2
                    calc_data_steps['lon_center'] = \
                        (calc_data_steps.site_a_longitude + calc_data_steps.site_b_longitude) / 2

                animation_rain_grids = []

                # interpolate each frame
                for x in range(calc_data_steps.time.size):
                    grid = interpolator(x=calc_data_steps.lon_center, y=calc_data_steps.lat_center,
                                        z=calc_data_steps.R.mean(dim='channel_id').isel(time=x),
                                        xgrid=x_grid, ygrid=y_grid)
                    animation_rain_grids.append(grid)

                    self.sig.progress_signal.emit({'prg_val': round((x / calc_data_steps.time.size) * 89) + 10})

                # emit output
                self.sig.plots_done_signal.emit({
                    "id": self.results_id,
                    "link_data": calc_data_steps,
                    "x_grid": x_grid,
                    "y_grid": y_grid,
                    "rain_grids": animation_rain_grids,
                })

        except BaseException as error:
            self.sig.error_signal.emit({"id": self.results_id})
            print(f"[CALC ID: {self.results_id}] ERROR: An unexpected error occurred during spatial interpolation: "
                  f"{type(error)} {error}.")
            print(f"[CALC ID: {self.results_id}] ERROR: Calculation thread terminated.")
            return

        print(f"[CALC ID: {self.results_id}] Rainfall calculation procedure ended.", flush=True)

    # noinspection PyMethodMayBeStatic
    def _fill_channel_dataset(self, curr_link, flux_data, tx_ip, rx_ip, channel_id, freq,
                              tx_zeros: bool = False, rx_zeros: bool = False) -> xr.Dataset:
        # get times from the Rx power array, since these data should be always available
        times = []
        for time in flux_data[rx_ip]["rx_power"].keys():
            times.append(np.datetime64(time).astype("datetime64[ns]"))

        # if creating empty channel dataset, fill Rx vars with zeros
        if rx_zeros:
            rsl = np.zeros((len(flux_data[rx_ip]["rx_power"]),), dtype=float)
            dummy = True
        else:
            rsl = [*flux_data[rx_ip]["rx_power"].values()]
            dummy = False

        # in case of Tx power zeros, get array length from Rx array since it should be always available
        if tx_zeros:
            tsl = np.zeros((len(flux_data[rx_ip]["rx_power"]),), dtype=float)
        else:
            tsl = [*flux_data[tx_ip]["tx_power"].values()]

        channel = xr.Dataset(
            data_vars={
                "tsl": ("time", tsl),
                "rsl": ("time", rsl),
            },
            coords={
                "time": times,
                "channel_id": channel_id,
                "cml_id": curr_link.link_id,
                "site_a_latitude": curr_link.latitude_a,
                "site_b_latitude": curr_link.latitude_b,
                "site_a_longitude": curr_link.longitude_a,
                "site_b_longitude": curr_link.longitude_b,
                "frequency": freq / 1000,
                "polarization": curr_link.polarization,
                "length": curr_link.distance,
                "dummy_channel": dummy,
                "dummy_a_latitude": curr_link.dummy_latitude_a,
                "dummy_b_latitude": curr_link.dummy_latitude_b,
                "dummy_a_longitude": curr_link.dummy_longitude_a,
                "dummy_b_longitude": curr_link.dummy_longitude_b,
            },
        )
        return channel
