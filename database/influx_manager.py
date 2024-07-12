from datetime import datetime
from enum import Enum
import math

from PyQt6.QtCore import QRunnable, pyqtSignal, QObject, QDateTime
from PyQt6.QtWidgets import QComboBox
from influxdb_client import InfluxDBClient, QueryApi, WriteApi
from influxdb_client.domain.write_precision import WritePrecision

from handlers import config_handler


class BucketType(Enum):
    """
    Enum specifying the type of InfluxDB bucket: 'default' or 'mapped'.
     - In case of 'mapped' bucket, bucket field names are mapped via MariaDB table 'technologies_influx_mapping'.
     - In case of 'default' bucket, default field names are used.
    """
    DEFAULT = 'default'
    MAPPED = 'mapped'


class InfluxManager:
    """
    InfluxManager class used for communication with InfluxDB database.
    """
    def __init__(self):
        super(InfluxManager, self).__init__()

        # create influx client with parameters from config file
        self.client: InfluxDBClient = InfluxDBClient.from_config_file(config_handler.config_path)
        self.qapi: QueryApi = self.client.query_api()
        self.wapi: WriteApi = self.client.write_api()

        data_border_format = '%Y-%m-%dT%H:%M:%SZ'
        data_border_string = config_handler.read_option('influx2', 'old_new_data_border')
        bucket_old_type = getattr(BucketType, config_handler.read_option('influx2', 'old_data_type'), BucketType.DEFAULT)
        bucket_new_type = getattr(BucketType, config_handler.read_option('influx2', 'new_data_type'), BucketType.DEFAULT)

        self.BUCKET_OLD_DATA: str = config_handler.read_option('influx2', 'bucket_old_data')
        self.BUCKET_NEW_DATA: str = config_handler.read_option('influx2', 'bucket_new_data')
        self.BUCKET_OUT_CML: str = config_handler.read_option('influx2', 'bucket_out_cml')
        self.BUCKET_OLD_TYPE: BucketType = bucket_old_type
        self.BUCKET_NEW_TYPE: BucketType = bucket_new_type
        self.OLD_NEW_DATA_BORDER: datetime = datetime.strptime(data_border_string, data_border_format)

    def check_connection(self) -> bool:
        return self.client.ping()

    def _raw_query_old_bucket(self, start_str: str, end_str: str, ips_str: str, interval_str: str) -> dict:
        # TODO: needs to be refactored to use the same query for both old and new buckets using BucketType enum
        # construct flux query
        flux = f"from(bucket: \"{self.BUCKET_OLD_DATA}\")\n" + \
               f"  |> range(start: {start_str}, stop: {end_str})\n" + \
               f"  |> filter(fn: (r) => r[\"_field\"] == \"rx_power\" or r[\"_field\"] == \"tx_power\" or" \
               f" r[\"_field\"] == \"temperature\")\n" + \
               f"  |> filter(fn: (r) => r[\"ip\"] =~ /{ips_str}/)\n" + \
               f"  |> aggregateWindow(every: {interval_str}, fn: mean, createEmpty: true)\n" + \
               f"  |> yield(name: \"mean\")"
        # print(f"History flux: {flux}")

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

    def _raw_query_new_bucket(self, start_str: str, end_str: str, ips_str: str, interval_str: str) -> dict:
        # TODO: needs to be refactored to use the same query for both old and new buckets using BucketType enum
        # construct flux query
        slux = f"from(bucket: \"{self.BUCKET_NEW_DATA}\")\n" + \
               f"  |> range(start: {start_str}, stop: {end_str})\n" + \
               f"  |> filter(fn: (r) => r[\"_field\"] == \"PrijimanaUroven\" or r[\"_field\"] == \"Signal\")\n" + \
               f"  |> filter(fn: (r) => r[\"agent_host\"] =~ /{ips_str}/)\n" + \
               f"  |> aggregateWindow(every: {interval_str}, fn: mean, createEmpty: true)\n" + \
               f"  |> yield(name: \"mean\")"

        flux = f"from(bucket: \"{self.BUCKET_NEW_DATA}\")\n" + \
               f"  |> range(start: {start_str}, stop: {end_str})\n" + \
               f"  |> filter(fn: (r) => r[\"_field\"] == \"Teplota\" or" \
               f" r[\"_field\"] == \"VysilaciVykon\" or r[\"_field\"] == \"Vysilany_Vykon\")\n" + \
               f"  |> filter(fn: (r) => r[\"agent_host\"] =~ /{ips_str}/)\n" + \
               f"  |> aggregateWindow(every: {interval_str}, fn: mean, createEmpty: true)\n" + \
               f"  |> yield(name: \"mean\")"

        # query influxDB
        results_slux = self.qapi.query(slux)
        results_flux = self.qapi.query(flux)

        data = {}

        # map raw fields to the result dictionary
        rename_map = {
            "Teplota": "temperature",
            "PrijimanaUroven": "rx_power",
            "VysilaciVykon": "tx_power",
            "Vysilany_Vykon": "tx_power",
            "Signal": "rx_power"
        }

        for results in (results_slux, results_flux):
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

    def query_units(self, ips: list, start: QDateTime, end: QDateTime, interval: int) -> dict:
        """
        Query InfluxDB for CMLs defined in 'ips' as list of their IP addresses (as identifiers = tags in InfluxDB).
        Query is done for the time interval defined by 'start' and 'end' QDateTime objects, with 'interval' in seconds.

        :param ips: list of IP addresses of CMLs to query
        :param start: QDateTime object with start of the query interval
        :param end: QDateTime object with end of the query interval
        :param interval: time interval in minutes
        :return: dictionary with queried data, with IP addresses as keys and fields with time series as values
        """
        # modify boundary times to be multiples of input time interval
        start.addSecs(((math.ceil((start.time().minute() + 0.1) / interval) * interval) - start.time().minute()) * 60)
        end.addSecs((-1 * (end.time().minute() % interval)) * 60)

        # convert params to query substrings
        start_str = start.toString("yyyy-MM-ddTHH:mm:00.000Z")  # RFC 3339
        end_str = end.toString("yyyy-MM-ddTHH:mm:00.000Z")  # RFC 3339
        interval_str = f"{interval * 60}s"  # time in seconds

        ips_str = f"{ips[0]}"  # IP addresses in query format
        for ip in ips[1:]:
            ips_str += f"|{ip}"

        if end.toPyDateTime() < self.OLD_NEW_DATA_BORDER:
            return self._raw_query_old_bucket(start_str, end_str, ips_str, interval_str)
        else:
            return self._raw_query_new_bucket(start_str, end_str, ips_str, interval_str)

    def query_units_realtime(self, ips: list, combo_realtime: QComboBox, interval: int) -> dict:
        """
        Query InfluxDB for CMLs defined in 'ips' as list of their IP addresses (as identifiers = tags in InfluxDB).
        Query is done for the time interval defined by 'combo_realtime' QComboBox object.

        :param ips: list of IP addresses of CMLs to query
        :param combo_realtime: QComboBox object with selected time interval string
        :param interval: time interval in minutes
        :return: dictionary with queried data, with IP addresses as keys and fields with time series as values
        """
        delta_map = {
            "Past 1 h": 1 * 3600,
            "Past 3 h": 3 * 3600,
            "Past 6 h": 6 * 3600,
            "Past 12 h": 12 * 3600,
            "Past 24 h": 24 * 3600,
            "Past 2 d": 48 * 3600,
            "Past 7 d": 168 * 3600,
            "Past 30 d": 720 * 3600
        }

        end = QDateTime.currentDateTimeUtc()
        start = end.addSecs(-1 * delta_map.get(combo_realtime.currentText()))

        return self.query_units(ips, start, end, interval)

    def write_points(self, points, bucket):
        self.wapi.write(bucket=bucket, record=points, write_precision=WritePrecision.S)

    def wipeout_output_bucket(self):
        self.client.delete_api().delete(
            start="1970-01-01T00:00:00Z",
            stop="2100-01-01T00:00:00Z",
            predicate='',
            bucket=self.BUCKET_OUT_CML
        )


class InfluxChecker(InfluxManager, QRunnable):
    """
    InfluxChecker class for connection testing with InfluxDB database. Use in threadpool.
    Emits 'ping_signal' from 'InfluxSignal' class passed as 'signals' parameter.
    """
    def __init__(self, signals: QObject):
        super(InfluxChecker, self).__init__()
        self.sig = signals

    def run(self):
        self.sig.ping_signal.emit(self.check_connection())


class InfluxSignals(QObject):
    """
    InfluxSignals class for signaling between InfluxManager and its threadpooled subclasses.
    """
    ping_signal = pyqtSignal(bool)
