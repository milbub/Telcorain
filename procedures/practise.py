from PyQt6.QtCore import QRunnable, pyqtSignal, QObject, QDateTime
from influxdb_client import InfluxDBClient
from influxdb_client.rest import ApiException


class InfluxManager:
    def __init__(self):
        # create influx client with parameters from config file
        self.client = InfluxDBClient.from_config_file("config.ini")
        self.qapi = self.client.query_api()

        # TODO: load value from settings
        self.bucket = "realtime_cbl"

    def check_connection(self) -> bool:
        return self.client.ping()

    # query influxDB for CMLs defined in 'ips' as list of their ip addresses
    # return their values in list of xarrays
    def query_signal_mean(self, ips: list, start: QDateTime, end: QDateTime, interval: int) -> dict:

        # convert params to query substrings
        start_str = start.toString("yyyy-MM-ddTHH:mm:ss.000Z")  # RFC 3339
        end_str = end.toString("yyyy-MM-ddTHH:mm:ss.000Z")  # RFC 3339
        interval_str = f"{interval * 60}s"  # time in seconds
        ips_str = f"r[\"agent_host\"] == \"{ips[0]}\""  # IP addresses in query format
        for ip in ips[1:]:
            ips_str += f" or r[\"agent_host\"] == \"{ip}\""
        print("chyba1")
        # construct flux query
        flux = f"from(bucket: \"{self.bucket}\")\n" + \
               f"  |> range(start: {start_str}, stop: {end_str})\n" + \
               f"  |> filter(fn: (r) => r[\"_field\"] == \"PrijimanaUroven\" or r[\"_field\"] == \"Teplota\" or" \
               f" r[\"_field\"] == \"VysilaciVykon\" or r[\"_field\"] == \"Vysilany_Vykon\" or r[\"_field\"] == \"Signal\")\n" + \
               f"  |> filter(fn: (r) => {ips_str})\n" + \
               f"  |> aggregateWindow(every: {interval_str}, fn: mean, createEmpty: true)\n" + \
               f"  |> yield(name: \"mean\")"
        #print("chyba2")
        #print(flux)

        # Replace old variable names with new variable names in the Flux query
        #flux = flux.replace('"Teplota"', '"temperature"')
        #flux = flux.replace('"PrijimanaUroven"', '"rx_power"')
        #flux = flux.replace('"VysilaciVykon"', '"tx_power"')
        #print(flux)

        # query influxDB
        results = self.qapi.query(flux)
        #print("Ide?")
        #results = results.replace('"Teplota"', '"temperature"')
        #results = results.replace('"PrijimanaUroven"', '"rx_power"')
        #results = results.replace('"VysilaciVykon"', '"tx_power"')
        #print(results)
        print("chyba3")
        # run rename function outside of Flux query
        #results = results.rename(columns={"temperature": "Teplota", "rx_power": "PrijimanaUroven", "tx_power": "Vysilaci_Vykon"})
        #print("chyba4")

        data = {}
        # rename fields in the result dictionary
        rename_map = {"Teplota": "temperature", "PrijimanaUroven": "rx_power", "VysilaciVykon": "tx_power", "Vysilany_Vykon": "tx_power", "Signal": "rx_power"}
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
                    else:
                        data[ip][field_name][record.get_time()] = record.get_value()

        print(f"Data z Influxu: {data}")
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
