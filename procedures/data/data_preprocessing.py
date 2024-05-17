from enum import Enum
from typing import Optional

import numpy as np
import xarray as xr

from procedures.calculation_signals import CalcSignals
from procedures.exceptions import ProcessingException


class ChannelIdentifiers(Enum):
    CHANNEL_0 = 'A(rx)_B(tx)'  # unit B (transmit) --> unit A (receive)
    CHANNEL_1 = 'B(rx)_A(tx)'  # unit A (transmit) --> unit B (receive)


def _fill_channel_dataset(
        current_link,
        flux_data,
        tx_ip,
        rx_ip,
        channel_id,
        freq,
        tx_zeros: bool = False,
        is_empty_channel: bool = False
) -> xr.Dataset:
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


def _sort_into_channels(
        influx_data: dict,
        links: dict,
        link_id: int,
        link_channel_selector: int,
        tx_ip: str,
        rx_ip: str,
        tx_tx_zeros: bool,
        tx_rx_zeros: bool,
        tx_freq: int,
        rx_freq: int,
        is_opposite_included: bool,
        channel_identifier: str,
        log_run_id: str
) -> Optional[list]:
    """
    Sorts link's data into channels and returns channels as a list of xarray datasets.
    """
    channels = []

    if (link_channel_selector in (1, 3)) and (rx_ip in influx_data):
        if not tx_tx_zeros:
            if len(influx_data[rx_ip]["rx_power"]) != len(influx_data[tx_ip]["tx_power"]):
                print(f"[{log_run_id}] WARNING: Skipping link ID: {link_id}. "
                      f"Non-coherent Rx/Tx data on channel {channel_identifier}.", flush=True)
                return None

        channel_a = _fill_channel_dataset(
            links[link_id],
            influx_data,
            tx_ip,
            rx_ip,
            channel_identifier,
            tx_freq,
            tx_tx_zeros
        )
        channels.append(channel_a)

        # if including only this channel, create empty second channel and fill it with zeros (pycomlink
        # functions require both channels included -> with this hack it's valid, but zeros have no effect)
        # rx and tx freqs are switched, since it's a (dummy) opposite channel
        if (link_channel_selector == 1) or not is_opposite_included:
            channel_b = _fill_channel_dataset(
                links[link_id],
                influx_data,
                rx_ip,
                rx_ip,  # rx will be always available (since it's a dummy channel, it doesn't matter)
                ChannelIdentifiers.CHANNEL_1 if channel_identifier == ChannelIdentifiers.CHANNEL_0
                else ChannelIdentifiers.CHANNEL_0,
                rx_freq,
                tx_rx_zeros,
                is_empty_channel=True
            )
            channels.append(channel_b)

        return channels


def convert_to_link_datasets(
        signals: CalcSignals,
        selected_links: dict,
        links: dict,
        influx_data: dict,
        missing_links: list,
        log_run_id: str,
        results_id: int
) -> list:
    """
    Merge raw influx data with link metadata and convert them into a list of xarray datasets, each representing a link.
    """
    link = 0

    try:
        calc_data = []
        link_count = len(selected_links)
        current_link = 0

        for link in selected_links:
            if selected_links[link] == 0:
                continue

            tx_zeros_b = False
            tx_zeros_a = False

            is_a_in = links[link].ip_a in influx_data
            is_b_in = links[link].ip_b in influx_data

            # TODO: load from options list of constant Tx power devices
            is_constant_tx_power = links[link].tech in ("1s10",)
            # TODO: load from options list of bugged techs with missing Tx zeros in InfluxDB
            is_tx_power_bugged = links[link].tech in ("ceragon_ip_10",)

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
            elif ("tx_power" not in influx_data[links[link].ip_a]) or \
                    ("tx_power" not in influx_data[links[link].ip_b]):
                # sadly, some devices of certain techs are badly exported from original source, and they are
                # missing Tx zero values in InfluxDB, so this hack needs to be done
                # (for other techs, there is no certainty, if original Tx value was zero in fact, or it's a NMS
                # error and these values are missing, so it's better to skip that links)
                if is_tx_power_bugged:
                    print(f"[{log_run_id}] INFO: Link ID: {link}. "
                          f"No Tx Power data available. Link technology \"{links[link].tech}\" is on "
                          f"exception list -> filling Tx data with zeros.", flush=True)
                    if "tx_power" not in influx_data[links[link].ip_b]:
                        tx_zeros_b = True
                    if "tx_power" not in influx_data[links[link].ip_a]:
                        tx_zeros_a = True
                else:
                    print(f"[{log_run_id}] INFO: Skipping link ID: {link}. "
                          f"No Tx Power data available.", flush=True)
                    # skip link
                    continue

            # hack: since one dimensional freq var in xarray is crashing pycomlink, change one freq negligibly to
            # preserve an array of two frequencies (channel A, channel B)
            if links[link].freq_a == links[link].freq_b:
                links[link].freq_a += 1

            link_channels = []

            # Side/unit A (channel B to A)
            unit_a_channels = _sort_into_channels(
                influx_data,
                links,
                link,
                selected_links[link],
                links[link].ip_b,
                links[link].ip_a,
                tx_zeros_b,
                tx_zeros_a,
                links[link].freq_b,
                links[link].freq_a,
                is_b_in,
                ChannelIdentifiers.CHANNEL_0.value,
                log_run_id
            )
            if unit_a_channels is not None:
                link_channels.extend(unit_a_channels)
            else:
                continue

            # Side/unit B (channel A to B)
            unit_b_channels = _sort_into_channels(
                influx_data,
                links,
                link,
                selected_links[link],
                links[link].ip_a,
                links[link].ip_b,
                tx_zeros_a,
                tx_zeros_b,
                links[link].freq_a,
                links[link].freq_b,
                is_a_in,
                ChannelIdentifiers.CHANNEL_1.value,
                log_run_id
            )
            if unit_b_channels is not None:
                link_channels.extend(unit_b_channels)
            else:
                continue

            calc_data.append(xr.concat(link_channels, dim="channel_id"))

            signals.progress_signal.emit({'prg_val': round((current_link / link_count) * 17) + 18})
            current_link += 1

        return calc_data

    except BaseException as error:
        signals.error_signal.emit({"id": results_id})
        print(f"[{log_run_id}] ERROR: An unexpected error occurred during data processing: "
              f"{type(error)} {error}.")
        print(f"[{log_run_id}] ERROR: Last processed microwave link ID: {link}")
        print(f"[{log_run_id}] ERROR: Calculation thread terminated.")

        raise ProcessingException("Error occured during influx data merging with metadata into xarray datasets.")
