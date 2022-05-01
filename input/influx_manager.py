from influxdb_client import InfluxDBClient


class InfluxManager:
    client = InfluxDBClient.from_config_file("config.ini")

    