"""Module containing class for handling MariaDB connection."""
from datetime import datetime
import json
from typing import Union

from PyQt6.QtCore import QRunnable, pyqtSignal, QObject
from mariadb import Cursor
import mariadb

from database.models.mwlink import MwLink
from procedures.utils.helpers import calc_distance

from handlers import config_handler
from handlers.logging_handler import logger

class SqlManager:
    """
    Class for handling MariaDB connection and database data loading/writing.
    """
    # Do not spam log with error messages
    is_error_sent = False

    def __init__(self):
        super(SqlManager, self).__init__()
        # Load settings from config file via ConfigurationManager
        self.settings = config_handler.load_sql_config()
        # Init empty connections
        self.connection = None
        # Define connection state
        self.is_connected = False

        # current realtime params DB ID
        self.realtime_params_id = 0

    def connect(self):
        """
        Connect to MariaDB database.
        """
        try:
            self.connection = mariadb.connect(
                user=self.settings["user"],
                password=self.settings["pass"],
                host=self.settings["address"],
                port=int(self.settings["port"]),
                database=self.settings["db_metadata"],
                connect_timeout=int(int(self.settings["timeout"]) / 1000),
                reconnect=True
            )

            self.is_connected = True
            SqlManager.is_error_sent = False

        except mariadb.Error as e:
            if not SqlManager.is_error_sent:
                logger.error("Cannot connect to MariaDB Platform: %s", e)
                SqlManager.is_error_sent = True
            self.is_connected = False

    def check_connection(self) -> bool:
        """
        Check connection state if it is still active.

        :return: True if connection is active, False otherwise.
        """
        if self.is_connected:
            try:
                self.connection.ping()
                return True
            except (mariadb.InterfaceError, mariadb.OperationalError):
                return False
        else:
            self.connect()
            return self.is_connected

    def load_metadata(self) -> dict[int, MwLink]:
        """
        Load metadata of CMLs from MariaDB.

        :return: Dictionary of CMLs metadata. Key is CML ID, value is MwLink model object.
        """
        try:
            if self.check_connection():
                cursor: Cursor = self.connection.cursor()

                query = """
                SELECT
                  links.ID,
                  links.IP_address_A,
                  links.IP_address_B,
                  links.frequency_A,
                  links.frequency_B,
                  links.polarization,
                  sites_A.address AS address_A,
                  sites_B.address AS address_B,
                  sites_A.X_coordinate AS longitude_A,
                  sites_B.X_coordinate AS longitude_B,
                  sites_A.Y_coordinate AS latitude_A,
                  sites_B.Y_coordinate AS latitude_B,
                  sites_A.X_dummy_coordinate AS dummy_longitude_A,
                  sites_B.X_dummy_coordinate AS dummy_longitude_B,
                  sites_A.Y_dummy_coordinate AS dummy_latitude_A,
                  sites_B.Y_dummy_coordinate AS dummy_latitude_B,
                  technologies.name AS technology_name,
                  technologies_influx_mapping.measurement AS technology_influx
                FROM
                  links
                JOIN sites AS sites_A ON links.site_A = sites_A.ID
                JOIN sites AS sites_B ON links.site_B = sites_B.ID
                JOIN technologies ON links.technology = technologies.ID
                JOIN technologies_influx_mapping ON technologies.influx_mapping_ID = technologies_influx_mapping.ID;
                """

                cursor.execute(query)

                links = {}

                for (ID, IP_address_A, IP_address_B, frequency_A, frequency_B, polarization, address_A, address_B,
                     longitude_A, longitude_B, latitude_A, latitude_B, dummy_longitude_A, dummy_longitude_B,
                     dummy_latitude_A, dummy_latitude_B, technology_name, technology_influx) in cursor:

                    link_length = calc_distance(latitude_A, longitude_A, latitude_B, longitude_B)

                    link = MwLink(
                        link_id=ID,
                        name=address_A + " <-> " + address_B,
                        tech=technology_influx,
                        name_a=address_A,
                        name_b=address_B,
                        freq_a=frequency_A,
                        freq_b=frequency_B,
                        polarization=polarization,
                        ip_a=IP_address_A,
                        ip_b=IP_address_B,
                        distance=link_length,
                        latitude_a=latitude_A,
                        longitude_a=longitude_A,
                        latitude_b=latitude_B,
                        longitude_b=longitude_B,
                        dummy_latitude_a=dummy_latitude_A,
                        dummy_longitude_a=dummy_longitude_A,
                        dummy_latitude_b=dummy_latitude_B,
                        dummy_longitude_b=dummy_longitude_B
                    )

                    links[ID] = link

                return links
            else:
                raise mariadb.Error("Connection is not active.")
        except mariadb.Error as e:
            logger.error("Failed to read data from MariaDB: %s", e)
            return {}

    def get_last_realtime(self) -> dict[str, Union[str, int, float, datetime]]:
        """
        Get parameters of last running realtime calculation from output database.

        :return: Dictionary of realtime parameters. Key is parameter name, value is parameter value.
        """
        try:
            if self.check_connection():
                cursor: Cursor = self.connection.cursor()

                query = "SELECT started, retention, timestep, resolution, X_MIN, X_MAX, Y_MIN, Y_MAX " \
                        f"FROM {self.settings['db_output']}.realtime_rain_parameters " \
                        "ORDER BY started DESC " \
                        "LIMIT 1;"

                cursor.execute(query)

                realtime_params = {}

                for (started, retention, timestep, resolution, X_MIN, X_MAX, Y_MIN, Y_MAX) in cursor:
                    realtime_params = {
                        "start_time": started,
                        "retention": retention,
                        "timestep": timestep,
                        "resolution": resolution,
                        "X_MIN": X_MIN,
                        "X_MAX": X_MAX,
                        "Y_MIN": Y_MIN,
                        "Y_MAX": Y_MAX
                    }

                return realtime_params
            else:
                raise mariadb.Error("Connection is not active.")
        except mariadb.Error as e:
            logger.error("Failed to read data from MariaDB: %s", e)
            return {}

    def insert_realtime(
            self,
            retention: int,
            timestep: int,
            resolution: float,
            X_MIN: float,
            X_MAX: float,
            Y_MIN: float,
            Y_MAX: float
    ):
        """
        Insert realtime parameters into output database.

        :param retention: Retention time in minutes.
        :param timestep: Timestep in seconds.
        :param resolution: Resolution in decimal degrees.
        :param X_MIN: Minimum longitude.
        :param X_MAX: Maximum longitude.
        :param Y_MIN: Minimum latitude.
        :param Y_MAX: Maximum latitude.
        """
        try:
            if self.check_connection():
                cursor: Cursor = self.connection.cursor()

                query = f"INSERT INTO {self.settings['db_output']}.realtime_rain_parameters " \
                        "(retention, timestep, resolution, X_MIN, X_MAX, Y_MIN, Y_MAX, X_count, Y_count, images_URL)" \
                        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"

                x = int((X_MAX - X_MIN) / resolution + 1)
                y = int((Y_MAX - Y_MIN) / resolution + 1)

                address = config_handler.read_option("realtime", "http_server_address")
                port = config_handler.read_option("realtime", "http_server_port")
                url = f"http://{address}:{port}"

                cursor.execute(query, (retention, timestep, resolution, X_MIN, X_MAX, Y_MIN, Y_MAX, x, y, url))
                self.connection.commit()

                # store the ID of the inserted record
                self.realtime_params_id = cursor.lastrowid
            else:
                raise mariadb.Error("Connection is not active.")
        except mariadb.Error as e:
            logger.error("Failed to insert data into MariaDB: %s", e)

    def get_last_raingrid(self) -> dict[datetime, list[int]]:
        """
        Get last raingrid from output database.

        :return: Dictionary of last raingrid. Key is time, value is list of CML IDs.
        """
        try:
            if self.check_connection():
                cursor: Cursor = self.connection.cursor()

                query = f"SELECT time, links FROM {self.settings['db_output']}.realtime_rain_grids " \
                        f"ORDER BY time DESC LIMIT 1;"

                cursor.execute(query)

                last_raingrid = {}

                for (time, links) in cursor:
                    last_raingrid[time] = json.loads(links)

                return last_raingrid
            else:
                raise mariadb.Error("Connection is not active.")
        except mariadb.Error as e:
            logger.error("Failed to read data from MariaDB: %s", e)
            return {}

    def insert_raingrid(self, time: datetime, links: list[int], file_name: str, r_min: float, r_max: float):
        """
        Insert raingrid's metadata into output database.

        :param time: Time of the raingrid.
        :param links: List of CML IDs.
        :param file_name: Name of the generated raingrid SVG image file.
        :param r_min: Minimum rain intensity value in given raingrid.
        :param r_max: Maximum rain intensity value in given raingrid.
        """
        if self.realtime_params_id == 0:
            raise ValueError("Unknown parameters ID. Realtime parameters has not been set?")

        try:
            if self.check_connection():
                cursor: Cursor = self.connection.cursor()

                query = (f"INSERT INTO {self.settings['db_output']}.realtime_rain_grids "
                         f"(time, parameters, links, image_name, R_MIN, R_MAX) VALUES (?, ?, ?, ?, ?, ?);")

                cursor.execute(query, (time, self.realtime_params_id, json.dumps(links), file_name, r_min, r_max))
                self.connection.commit()
            else:
                raise mariadb.Error("Connection is not active.")
        except mariadb.Error as e:
            logger.error("Failed to insert data into MariaDB: %s", e)

    def wipeout_realtime_tables(self):
        """
        Truncate realtime tables in output database.
        """
        try:
            if self.check_connection():
                cursor: Cursor = self.connection.cursor()

                queries = (
                    "SET FOREIGN_KEY_CHECKS = 0;",
                    f"TRUNCATE TABLE {self.settings['db_output']}.realtime_rain_grids;",
                    f"TRUNCATE TABLE {self.settings['db_output']}.realtime_rain_parameters;",
                    "SET FOREIGN_KEY_CHECKS = 1;"
                )

                for query in queries:
                    cursor.execute(query)
                self.connection.commit()
            else:
                raise mariadb.Error("Connection is not active.")
        except mariadb.Error as e:
            logger.error("Failed to insert data into MariaDB: %s", e)
        else:
            logger.info("[DEVMODE] MariaDB output tables erased.")

    def __del__(self):
        self.connection.close()


class SqlChecker(SqlManager, QRunnable):
    """
    Subclass for use in threadpool, for connection testing.
    Emits 'ping_signal' from 'SqlSignal' class passed as 'signals' parameter.
    """
    def __init__(self, signals: QObject):
        super(SqlChecker, self).__init__()
        self.sig = signals

    def run(self):
        self.sig.ping_signal.emit(self.check_connection())


class SqlSignals(QObject):
    """
    Signaling class for SqlManager's threadpool subclasses.
    """
    ping_signal = pyqtSignal(bool)
