import time

from PyQt6.QtCore import QRunnable, pyqtSignal, QObject, QDateTime
from PyQt6.QtWidgets import QComboBox
from influxdb_client import InfluxDBClient
from influxdb_client.domain.write_precision import WritePrecision
from datetime import datetime, timedelta
import math


class InfluxManager:
    def __init__(self, config_man):
        super(InfluxManager, self).__init__()

        # create influx client with parameters from config file
        self.client = InfluxDBClient.from_config_file(config_man.config_path)
        self.qapi = self.client.query_api()
        self.wapi = self.client.write_api()

        self.BUCKET_OLD_DATA = config_man.read_option('influx2', 'bucket_old_data')
        self.BUCKET_NEW_DATA = config_man.read_option('influx2', 'bucket_new_data')
        self.BUCKET_OUT_CML = config_man.read_option('influx2', 'bucket_out_cml')

    def check_connection(self) -> bool:
        return self.client.ping()

    # query influxDB for CMLs defined in 'ips' as list of their ip addresses
    # return their values in list of xarrays
    def query_signal_mean(self, ips: list, start: QDateTime, end: QDateTime, interval: int) -> dict:
        # modify boundary times to be multiples of input time interval
        start.addSecs(((math.ceil((start.time().minute() + 0.1) / interval) * interval) - start.time().minute()) * 60)
        end.addSecs((-1 * (end.time().minute() % interval)) * 60)

        # convert params to query substrings
        start_str = start.toString("yyyy-MM-ddTHH:mm:00.000Z")  # RFC 3339
        end_str = end.toString("yyyy-MM-ddTHH:mm:00.000Z")  # RFC 3339
        interval_str = f"{interval * 60}s"  # time in seconds
        ips_str = f"r[\"ip\"] == \"{ips[0]}\""  # IP addresses in query format
        for ip in ips[1:]:
            ips_str += f" or r[\"ip\"] == \"{ip}\""

        # construct flux query
        flux = f"from(bucket: \"{self.BUCKET_OLD_DATA}\")\n" + \
               f"  |> range(start: {start_str}, stop: {end_str})\n" + \
               f"  |> filter(fn: (r) => r[\"_field\"] == \"rx_power\" or r[\"_field\"] == \"tx_power\" or" \
               f" r[\"_field\"] == \"temperature\")\n" + \
               f"  |> filter(fn: (r) => {ips_str})\n" + \
               f"  |> aggregateWindow(every: {interval_str}, fn: mean, createEmpty: true)\n" + \
               f"  |> yield(name: \"mean\")"
        print(f"History flux: {flux}")
        # query influxDB
        results = self.qapi.query(flux)

        data = {}
        for table in results:
            ip = table.records[0].values.get("ip")

            # initialize new IP record in the result dictionary
            if ip not in data:
                data[ip] = {}
                data[ip]['unit'] = table.records[0].get_measurement()

            # collect data from the current table
            for record in table.records:
                if ip in data:
                    if record.get_field() not in data[ip]:
                        data[ip][record.get_field()] = {}

                    # correct bad Tx Power and Temperature data in InfluxDB in case of missing zero values
                    if (record.get_field() == 'tx_power') and (record.get_value() is None):
                        data[ip]['tx_power'][record.get_time()] = 0.0
                    elif (record.get_field() == 'temperature') and (record.get_value() is None):
                        data[ip]['temperature'][record.get_time()] = 0.0
                    elif (record.get_field() == 'rx_power') and (record.get_value() is None):
                        data[ip]['rx_power'][record.get_time()] = 0.0
                    else:
                        data[ip][record.get_field()][record.get_time()] = record.get_value()

        return data

    def query_signal_mean_realtime(self, ips: list, combo_realtime: QComboBox, interval: int) -> dict:
        delta_map = {
            "Past 1 h": timedelta(hours=1),
            "Past 3 h": timedelta(hours=3),
            "Past 6 h": timedelta(hours=6),
            "Past 12 h": timedelta(hours=12),
            "Past 24 h": timedelta(hours=24),
            "Past 2 d": timedelta(days=2),
            "Past 7 d": timedelta(days=7),
            "Past 30 d": timedelta(days=30)
        }

        end = datetime.utcnow()
        delta = delta_map.get(combo_realtime)

        if delta is None:
            raise ValueError(f"Invalid delta value: {delta}")

        start = end - delta

        # modify boundary times to be multiples of input time interval
        start_c = timedelta(minutes=((math.ceil((start.time().minute + 0.1) / interval) * interval) - start.time().minute))
        start = start + start_c
        end_c = timedelta(minutes=(end.time().minute % interval))
        end = end - end_c

        # convert params to query substrings
        start_str = start.strftime("%Y-%m-%dT%H:%M:00.000Z")  # RFC 3339
        end_str = end.strftime("%Y-%m-%dT%H:%M:00.000Z")  # RFC 3339

        interval_str = f"{interval * 60}s"  # time in seconds
        ips_str = f"r[\"agent_host\"] == \"{ips[0]}\""  # IP addresses in query format
        for ip in ips[1:]:
            ips_str += f" or r[\"agent_host\"] == \"{ip}\""

        # construct flux query
        flux = f"from(bucket: \"{self.BUCKET_NEW_DATA}\")\n" + \
               f"  |> range(start: {start_str}, stop: {end_str})\n" + \
               f"  |> filter(fn: (r) => r[\"_field\"] == \"PrijimanaUroven\" or r[\"_field\"] == \"Teplota\" or" \
               f" r[\"_field\"] == \"VysilaciVykon\" or r[\"_field\"] == \"Vysilany_Vykon\" or r[\"_field\"] == \"Signal\")\n" + \
               f"  |> filter(fn: (r) => {ips_str})\n" + \
               f"  |> aggregateWindow(every: {interval_str}, fn: mean, createEmpty: true)\n" + \
               f"  |> yield(name: \"mean\")"
        print(f"RealTime flux: {flux}")
        # query influxDB
        results = self.qapi.query(flux)

        data = {}
        # rename fields in the result dictionary
        rename_map = {"Teplota": "temperature", "PrijimanaUroven": "rx_power", "VysilaciVykon": "tx_power",
                      "Vysilany_Vykon": "tx_power", "Signal": "rx_power"}
        for table in results:
            ip = table.records[0].values.get("agent_host")

            # initialize new IP record in the result dictionary
            if ip not in data:
                data[ip] = {}
                data[ip]['unit'] = table.records[0].get_measurement()

            # collect data from the current table
            for record in table.records:
                if ip in data:
                    field_name = rename_map.get(record.get_field(), record.get_field())
                    if field_name not in data[ip]:
                        data[ip][field_name] = {}

                    # correct bad Tx Power and Temperature data in InfluxDB in case of missing zero values
                    if (field_name == 'tx_power') and (record.get_value() is None):
                        data[ip]['tx_power'][record.get_time()] = 0.0
                    elif (field_name == 'temperature') and (record.get_value() is None):
                        data[ip]['temperature'][record.get_time()] = 0.0
                    elif (field_name == 'rx_power') and (record.get_value() is None):
                        data[ip]['rx_power'][record.get_time()] = 0.0
                    else:
                        data[ip][field_name][record.get_time()] = record.get_value()

        return data

    def write_points(self, points, bucket):
        self.wapi.write(bucket=bucket, record=points, write_precision=WritePrecision.S)


class InfluxChecker(InfluxManager, QRunnable):
    # subclass for use in threadpool, for connection testing
    # emits 'ping_signal' from 'InfluxSignal' class passed as 'signals' parameter
    def __init__(self, config_man, signals: QObject):
        super(InfluxChecker, self).__init__(config_man)
        self.sig = signals

    def run(self):
        self.sig.ping_signal.emit(self.check_connection())


class InfluxSignals(QObject):
    # signaling class for InfluxManager's threadpool subclasses
    ping_signal = pyqtSignal(bool)
