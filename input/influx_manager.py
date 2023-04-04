from PyQt6.QtCore import QRunnable, pyqtSignal, QObject, QDateTime
from influxdb_client import InfluxDBClient


class InfluxManager:
    def __init__(self):
        # create influx client with parameters from config file
        self.client = InfluxDBClient.from_config_file("config.ini")
        self.qapi = self.client.query_api()

        # TODO: load value from settings
        self.bucket = "mws"

    def check_connection(self) -> bool:
        return self.client.ping()

    # query influxDB for CMLs defined in 'ips' as list of their ip addresses
    # return their values in list of xarrays
    def query_signal_mean(self, ips: list, start: QDateTime, end: QDateTime, interval: int) -> dict:

        # convert params to query substrings
        start_str = start.toString("yyyy-MM-ddTHH:mm:ss.000Z")  # RFC 3339
        end_str = end.toString("yyyy-MM-ddTHH:mm:ss.000Z")      # RFC 3339
        interval_str = f"{interval * 60}s"                      # time in seconds
        ips_str = f"r[\"ip\"] == \"{ips[0]}\""                  # IP addresses in query format
        for ip in ips[1:]:
            ips_str += f" or r[\"ip\"] == \"{ip}\""

        # construct flux query
        flux = f"from(bucket: \"{self.bucket}\")\n" + \
               f"  |> range(start: {start_str}, stop: {end_str})\n" + \
               f"  |> filter(fn: (r) => r[\"_field\"] == \"rx_power\" or r[\"_field\"] == \"tx_power\")\n" + \
               f"  |> filter(fn: (r) => {ips_str})\n" + \
               f"  |> aggregateWindow(every: {interval_str}, fn: mean, createEmpty: true)\n" + \
               f"  |> yield(name: \"mean\")"

        # query influxDB
        results = self.qapi.query(flux)

        data = {}
        for table in results:
            ip = table.records[0].values.get("ip")

            if ip not in data:
                data[ip] = {}
                data[ip]['unit'] = table.records[0].get_measurement()

            for record in table.records:
                if ip in data:
                    if record.get_field() not in data[ip]:
                        data[ip][record.get_field()] = {}

                    # correct bad Tx Power data in InfluxDB in case of missing zero values
                    if (record.get_field() == 'tx_power') and (record.get_value() is None):
                        data[ip]['tx_power'][record.get_time()] = 0.0
                    else:
                        data[ip][record.get_field()][record.get_time()] = record.get_value()

        return data


class InfluxChecker(QRunnable, InfluxManager):
    # subclass for use in threadpool, for connection testing
    # emits 'ping_signal' from 'InfluxSignal' class passed as 'signals' parameter
    def __init__(self, signals: QObject):
        QRunnable.__init__(self)
        InfluxManager.__init__(self)
        self.sig = signals

    def run(self):
        self.sig.ping_signal.emit(self.check_connection())


class InfluxSignals(QObject):
    # signaling class for InfluxManager's threadpool subclasses
    ping_signal = pyqtSignal(bool)
