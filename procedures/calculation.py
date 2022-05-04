import numpy as np
import pycomlink as pycml
import xarray as xr
from PyQt6.QtCore import QRunnable, QObject, QDateTime, pyqtSignal

import input.influx_manager as influx


class CalcSignals(QObject):
    done_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(dict)
    progress_signal = pyqtSignal(dict)


def _channel_dataset(curr_link, flux_data, tx_ip, rx_ip, channel_id, freq, tx_zeros: bool) -> xr.Dataset:
    times = []
    for time in flux_data[rx_ip]["rx_power"].keys():
        times.append(np.datetime64(time).astype("datetime64[ns]"))

    if tx_zeros:
        tsl = np.zeros((len(flux_data[rx_ip]["rx_power"]),), dtype=float)
    else:
        tsl = [*flux_data[tx_ip]["tx_power"].values()]

    channel = xr.Dataset(
        data_vars={
            "tsl": ("time", tsl),
            "rsl": ("time", [*flux_data[rx_ip]["rx_power"].values()]),
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
        },
    )
    return channel


class Calculation(QRunnable):
    def __init__(self, signals: CalcSignals, results_id: int, links: dict, selection: dict, start: QDateTime,
                 end: QDateTime, interval: int, rolling_vals: int):
        QRunnable.__init__(self)
        self.sig = signals
        self.results_id = results_id
        self.links = links
        self.selection = selection
        self.start = start
        self.end = end
        self.interval = interval
        self.rolling_vals = rolling_vals

    def run(self):
        print(f"[CALC ID: {self.results_id}] Rainfall calculation procedure started.", flush=True)

        # ////// DATA ACQUISITION \\\\\\

        try:

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

                if (self.links[link].ip_a not in influx_data) or (self.links[link].ip_b not in influx_data):
                    if link not in missing_links:
                        print(f"[CALC ID: {self.results_id}] INFO: Skipping link ID: {link}. "
                              f"No unit data available.", flush=True)
                    continue
                else:
                    # TODO: add dynamic exception list of constant Tx power devices
                    # skip links with missing Tx power data on the one of the units (unable to do Tx power correction)
                    # Orcaves 1S10 and IP10Gs have constant Tx power, so it doesn't matter
                    if self.links[link].tech in ("1s10", "ip20G"):
                        tx_zeros_b = True
                        tx_zeros_a = True
                    elif ("tx_power" not in influx_data[self.links[link].ip_a]) or \
                         ("tx_power" not in influx_data[self.links[link].ip_b]):
                        # TODO: add dynamic exception list of bugged techs with missing Tx zeros in InfluxDB
                        # sadly, some devices of certain techs are badly exported from original source and they are
                        # missing Tx zero values in InfluxDB, so this hack needs to be done
                        # (for other techs, there is no certainty, if original Tx value was zero in fact or it's a NMS
                        # error and these values are missing, so it's better to skip that links)
                        if self.links[link].tech == "ip10":
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
                            continue

                link_channels = []

                # Side/unit A (channel B to A)
                if (self.selection[link] in (1, 3)) and (self.links[link].ip_a in influx_data):
                    if not tx_zeros_b:
                        if len(influx_data[self.links[link].ip_a]["rx_power"])\
                                != len(influx_data[self.links[link].ip_b]["tx_power"]):
                            print(f"[CALC ID: {self.results_id}] WARNING: Skipping link ID: {link}. "
                                  f"Non-coherent Rx/Tx data on channel A(rx)_B(tx).", flush=True)
                            continue
                    channel_a = _channel_dataset(self.links[link], influx_data, self.links[link].ip_b,
                                                 self.links[link].ip_a, 'A(rx)_B(tx)', self.links[link].freq_b,
                                                 tx_zeros_b)
                    link_channels.append(channel_a)

                # Side/unit B (channel A to B)
                if (self.selection[link] in (2, 3)) and (self.links[link].ip_b in influx_data):
                    if not tx_zeros_a:
                        if len(influx_data[self.links[link].ip_b]["rx_power"])\
                                != len(influx_data[self.links[link].ip_a]["tx_power"]):
                            print(f"[CALC ID: {self.results_id}] WARNING: Skipping link ID: {link}. "
                                  f"Non-coherent Rx/Tx data on channel B(rx)_A(tx).", flush=True)
                            continue

                    # hack: since one dimensional freq data in xarray are crashing pycomlink, change one freq negligibly
                    if self.links[link].freq_a == self.links[link].freq_b:
                        self.links[link].freq_a += 1

                    channel_b = _channel_dataset(self.links[link], influx_data, self.links[link].ip_a,
                                                 self.links[link].ip_b, 'B(rx)_A(tx)', self.links[link].freq_a,
                                                 tx_zeros_a)
                    link_channels.append(channel_b)

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

            # interpolate gaps in input data, filter out nonsenses out of limits
            print(f"[CALC ID: {self.results_id}] Smoothing signal data...")
            link_count = len(calc_data)
            curr_link = 0

            for cml in calc_data:
                # TODO: load upper tx power from options (here it's 99 dBm)
                cml['tsl'] = cml.tsl.astype(float).where(cml.tsl < 99.0)
                cml['tsl'] = cml.tsl.astype(float).interpolate_na(dim='time', method='linear', max_gap='5min')
                # TODO: load bottom rx power from options (here it's -80 dBm)
                cml['rsl'] = cml.rsl.astype(float).where(cml.rsl != 0.0).where(cml.rsl > -80.0)
                cml['rsl'] = cml.rsl.astype(float).interpolate_na(dim='time', method='linear', max_gap='5min')

                cml['trsl'] = cml.tsl - cml.rsl
                cml['trsl'] = cml.trsl.astype(float).interpolate_na(dim='time', method='nearest', max_gap='5min')
                cml['trsl'] = cml.trsl.astype(float).fillna(0.0)

                self.sig.progress_signal.emit({'prg_val': round((curr_link / link_count) * 15) + 35})
                curr_link += 1

            # process each link:
            print(f"[CALC ID: {self.results_id}] Computing rain values...")
            curr_link = 0

            for cml in calc_data:

                # determine wet periods
                cml['wet'] = cml.trsl.rolling(time=self.rolling_vals, center=True).std(skipna=False) > 0.8

                # calculate ratio of wet periods
                cml['wet_fraction'] = (cml.wet == 1).sum() / (cml.wet == 0).sum()

                # determine signal baseline
                cml['baseline'] = pycml.processing.baseline.baseline_constant(trsl=cml.trsl, wet=cml.wet,
                                                                              n_average_last_dry=5)

                # calculate wet antenna attenuation
                cml['waa'] = pycml.processing.wet_antenna.waa_schleiss_2013(rsl=cml.trsl, baseline=cml.baseline,
                                                                            wet=cml.wet, waa_max=1.55, delta_t=1,
                                                                            tau=15)

                # calculate final rain attenuation
                cml['A'] = cml.trsl - cml.baseline - cml.waa

                # calculate rain intensity
                cml['R'] = pycml.processing.k_R_relation.calc_R_from_A(A=cml.A, L_km=float(cml.length),
                                                                       f_GHz=cml.frequency, pol=cml.polarization)

                self.sig.progress_signal.emit({'prg_val': round((curr_link / link_count) * 40) + 50})
                curr_link += 1

            print(f"[CALC ID: {self.results_id}] Resampling rain values for rainfall total...")

            cmls_rain_1h = xr.concat(objs=[cml.R.resample(time='1h', label='right').mean() for cml in calc_data],
                                     dim='cml_id').to_dataset()

            self.sig.progress_signal.emit({'prg_val': 93})

            print(f"[CALC ID: {self.results_id}] Interpolating spatial data...")

            cmls_rain_1h['lat_center'] = (cmls_rain_1h.site_a_latitude + cmls_rain_1h.site_b_latitude) / 2
            cmls_rain_1h['lon_center'] = (cmls_rain_1h.site_a_longitude + cmls_rain_1h.site_b_longitude) / 2

            interpolator = pycml.spatial.interpolator.IdwKdtreeInterpolator(nnear=50, p=1, exclude_nan=False,
                                                                            max_distance=1)

            rain_grid = interpolator(x=cmls_rain_1h.lon_center, y=cmls_rain_1h.lat_center,
                                     z=cmls_rain_1h.R.mean(dim='channel_id').sum(dim='time'), resolution=0.001)

            self.sig.progress_signal.emit({'prg_val': 99})

        except BaseException as error:
            self.sig.error_signal.emit({"id": self.results_id})
            print(f"[CALC ID: {self.results_id}] ERROR: An unexpected error occurred during rain calculation: "
                  f"{type(error)} {error}.")
            print(f"[CALC ID: {self.results_id}] ERROR: Last processed microwave link dataset: {calc_data[curr_link]}")
            print(f"[CALC ID: {self.results_id}] ERROR: Calculation thread terminated.")
            return

        # ////// EMIT OUTPUT \\\\\\

        self.sig.done_signal.emit({
            "id": self.results_id,
            "data": calc_data,
            "interpolator": interpolator,
            "rain_grid": rain_grid,
            "cmls_rain_1h": cmls_rain_1h
        })

    def __del__(self):
        print(f"[CALC ID: {self.results_id}] Rainfall calculation procedure ended.", flush=True)
