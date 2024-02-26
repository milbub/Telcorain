from database.influx_manager import InfluxManager
from procedures.calculation_signals import CalcSignals


def _get_ips_from_links_dict(selected_links: dict, links: dict) -> list:
    if len(selected_links) < 1:
        raise ValueError('Empty selection array.')

    ips = []
    for link in selected_links:
        if link in links:
            # TODO: add dynamic exception list of constant Tx power devices
            # 1S10s have constant Tx power, so only one unit can be included in query
            # otherwise, both ends needs to be included in query, due Tx power correction
            if links[link].tech in "1s10":
                if selected_links[link] == 1:
                    ips.append(links[link].ip_a)
                elif selected_links[link] == 2:
                    ips.append(links[link].ip_b)
                elif selected_links[link] == 3:
                    ips.append(links[link].ip_a)
                    ips.append(links[link].ip_b)
            elif selected_links[link] == 0:
                continue
            else:
                ips.append(links[link].ip_a)
                ips.append(links[link].ip_b)
    
    return ips


def load_data_from_influxdb(
        influx_man: InfluxManager,
        signals: CalcSignals,
        cp: dict,
        selected_links: dict,
        links: dict,
        log_run_id: str
) -> (dict, list, list):
    ips = _get_ips_from_links_dict(selected_links, links)

    signals.progress_signal.emit({'prg_val': 5})
    print(f"[{log_run_id}] Querying InfluxDB for selected microwave links data...", flush=True)

    # Realtime calculation is being done
    if cp['is_realtime']:
        print(f"[{log_run_id}] Realtime data procedure started.", flush=True)
        influx_data = influx_man.query_signal_mean_realtime(ips, cp['realtime_timewindow'], cp['step'])

    # In other case, notify we are doing historic calculation
    else:
        print(f"[{log_run_id}] Historic data procedure started.", flush=True)
        influx_data = influx_man.query_signal_mean(ips, cp['start'], cp['end'], cp['step'])

    diff = len(ips) - len(influx_data)

    signals.progress_signal.emit({'prg_val': 15})
    print(f"[{log_run_id}] Querying done. Got data of {len(influx_data)} units,"
          f" of total {len(ips)} selected units.")

    missing_links = []
    if diff > 0:
        print(f"[{log_run_id}] {diff} units are not available in selected time window:")
        for ip in ips:
            if ip not in influx_data:
                for link in links:
                    if links[link].ip_a == ip:
                        print(f"[{log_run_id}] Link: {links[link].link_id}; "
                              f"Tech: {links[link].tech}; SIDE A: {links[link].name_a}; "
                              f"IP: {links[link].ip_a}")
                        missing_links.append(link)
                        break
                    elif links[link].ip_b == ip:
                        print(f"[{log_run_id}] Link: {links[link].link_id}; "
                              f"Tech: {links[link].tech}; SIDE B: {links[link].name_b}; "
                              f"IP: {links[link].ip_b}")
                        missing_links.append(link)
                        break

    signals.progress_signal.emit({'prg_val': 18})

    return influx_data, missing_links, ips
