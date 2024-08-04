from datetime import datetime
from enum import Enum
import traceback
from typing import Optional, Union

import numpy as np
import xarray as xr

from database.models.mwlink import MwLink
from handlers.logging_handler import logger
from procedures.calculation_signals import CalcSignals
from procedures.exceptions import ProcessingException


class ChannelIdentifier(Enum):
    CHANNEL_0 = "A(rx)_B(tx)"  # unit B (transmit) --> unit A (receive)
    CHANNEL_1 = "B(rx)_A(tx)"  # unit A (transmit) --> unit B (receive)


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

    # if creating empty channel dataset, fill data vars with zeros
    if is_empty_channel:
        rsl = np.zeros((len(flux_data[rx_ip]["rx_power"]),), dtype=float)

        # => get array length from rx_power of rx_ip, since it should be always defined
        temperature_rx = np.zeros((len(flux_data[rx_ip]["rx_power"]),), dtype=float)
        temperature_tx = np.zeros((len(flux_data[rx_ip]["rx_power"]),), dtype=float)

        dummy = True
    else:
        rsl = [*flux_data[rx_ip]["rx_power"].values()]

        # temperature data can be missing in some cases, if so, fill with zeros
        if "temperature" in flux_data[rx_ip].keys():
            temperature_rx = [*flux_data[rx_ip]["temperature"].values()]
        else:
            temperature_rx = np.zeros((len(flux_data[rx_ip]["rx_power"]),), dtype=float)
        if tx_ip in flux_data and "temperature" in flux_data[tx_ip].keys():
            temperature_tx = [*flux_data[tx_ip]["temperature"].values()]
        else:
            temperature_tx = np.zeros((len(flux_data[rx_ip]["rx_power"]),), dtype=float)

        dummy = False

    # in case of Tx power zeros, we don't have data of Tx unit available in flux_data
    if tx_zeros:
        # => get array length from rx_power of rx_ip, since it should be always defined
        tsl = np.zeros((len(flux_data[rx_ip]["rx_power"]),), dtype=float)
    else:
        tsl = [*flux_data[tx_ip]["tx_power"].values()]

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
        influx_data: dict[str, Union[dict[str, dict[datetime, float]], str]],
        links: dict[int, MwLink],
        link_id: int,
        link_channel_selector: int,
        tx_ip: str,
        rx_ip: str,
        tx_tx_zeros: bool,
        tx_rx_zeros: bool,
        tx_freq: int,
        rx_freq: int,
        is_opposite_included: bool,
        channel_identifier: ChannelIdentifier,
        log_run_id: str
) -> Optional[list]:
    """
    Sorts link's data into channels and returns channels as a list of xarray datasets.
    """
    channels = []

    # ChannelIdentifier -> channel_selector mapping
    channel_selector_map = {
        ChannelIdentifier.CHANNEL_0: 1,
        ChannelIdentifier.CHANNEL_1: 2
    }
    all_channels_selector = 3

    if (link_channel_selector in (channel_selector_map.get(channel_identifier), all_channels_selector)) and (rx_ip in influx_data):
        if not tx_tx_zeros:
            if len(influx_data[rx_ip]["rx_power"]) != len(influx_data[tx_ip]["tx_power"]):
                logger.warning(
                    "[%s] Skipping link ID: %d. Non-coherent Rx/Tx data on channel %s.",
                    log_run_id, link_id, channel_identifier.value
                )
                return None

        channel_a = _fill_channel_dataset(
            current_link=links[link_id],
            flux_data=influx_data,
            tx_ip=tx_ip,
            rx_ip=rx_ip,
            channel_id=channel_identifier.value,
            freq=tx_freq,
            tx_zeros=tx_tx_zeros
        )
        channels.append(channel_a)

        # if including only this channel, create empty second channel and fill it with zeros (pycomlink
        # functions require both channels included -> with this hack it's valid, but zeros have no effect)
        # rx and tx freqs are switched, since it's a (dummy) opposite channel
        if (link_channel_selector == channel_selector_map.get(channel_identifier)) or not is_opposite_included:
            channel_b = _fill_channel_dataset(
                current_link=links[link_id],
                flux_data=influx_data,
                tx_ip=rx_ip,  # rx will be always available (since it's a dummy channel, it doesn't matter)
                rx_ip=rx_ip,
                channel_id=ChannelIdentifier.CHANNEL_1.value if channel_identifier == ChannelIdentifier.CHANNEL_0
                else ChannelIdentifier.CHANNEL_0.value,
                freq=rx_freq,
                tx_zeros=tx_rx_zeros,
                is_empty_channel=True
            )
            channels.append(channel_b)

        return channels


def convert_to_link_datasets(
        signals: CalcSignals,
        selected_links: dict[int, int],
        links: dict[int, MwLink],
        influx_data: dict[str, Union[dict[str, dict[datetime, float]], str]],
        missing_links: list[int],
        log_run_id: str,
        results_id: int
) -> list[xr.Dataset]:
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
            is_constant_tx_power = links[link].tech in ("1s10", "summit", "summit_bt")
            # TODO: load from options list of bugged techs with missing Tx zeros in InfluxDB
            is_tx_power_bugged = links[link].tech in ("ceragon_ip_10",)

            # skip links, where data of one unit (or both) are not available
            # but constant Tx power devices are exceptions
            if not (is_a_in and is_b_in):
                if not ((is_a_in != is_b_in) and is_constant_tx_power):
                    if link not in missing_links:
                        logger.debug("[%s] Skipping link ID: %d. No unit data available.", log_run_id, link)
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
                    logger.debug(
                        "[%s] Link ID: %d. No Tx Power data available. Link technology \"%s\" is on"
                        " exception list -> filling Tx data with zeros.",
                        log_run_id, link, links[link].tech
                    )
                    if "tx_power" not in influx_data[links[link].ip_b]:
                        tx_zeros_b = True
                    if "tx_power" not in influx_data[links[link].ip_a]:
                        tx_zeros_a = True
                else:
                    logger.debug("[%s] Skipping link ID: %d. No Tx Power data available.", log_run_id, link)
                    # skip link
                    continue

            # hack: since one dimensional freq var in xarray is crashing pycomlink, change one freq negligibly to
            # preserve an array of two frequencies (channel A, channel B)
            if links[link].freq_a == links[link].freq_b:
                links[link].freq_a += 1

            link_channels = []

            # Side/unit A (channel B to A)
            unit_a_channels = _sort_into_channels(
                influx_data=influx_data,
                links=links,
                link_id=link,
                link_channel_selector=selected_links[link],
                tx_ip=links[link].ip_b,
                rx_ip=links[link].ip_a,
                tx_tx_zeros=tx_zeros_b,
                tx_rx_zeros=tx_zeros_a,
                tx_freq=links[link].freq_b,
                rx_freq=links[link].freq_a,
                is_opposite_included=is_b_in,
                channel_identifier=ChannelIdentifier.CHANNEL_0,
                log_run_id=log_run_id
            )
            if unit_a_channels is not None:
                link_channels.extend(unit_a_channels)
            else:
                continue

            # Side/unit B (channel A to B)
            unit_b_channels = _sort_into_channels(
                influx_data=influx_data,
                links=links,
                link_id=link,
                link_channel_selector=selected_links[link],
                tx_ip=links[link].ip_a,
                rx_ip=links[link].ip_b,
                tx_tx_zeros=tx_zeros_a,
                tx_rx_zeros=tx_zeros_b,
                tx_freq=links[link].freq_a,
                rx_freq=links[link].freq_b,
                is_opposite_included=is_a_in,
                channel_identifier=ChannelIdentifier.CHANNEL_1,
                log_run_id=log_run_id
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

        logger.error(
            "[%s] An unexpected error occurred during data processing: %s %s.\n"
            "Last processed microwave link ID: %d\n"
            "Calculation thread terminated.",
            log_run_id, type(error), error, link
        )

        traceback.print_exc()

        raise ProcessingException("Error occured during influx data merging with metadata into xarray datasets.")
