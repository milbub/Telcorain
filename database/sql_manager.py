from datetime import datetime
import json

from PyQt6.QtCore import QRunnable, pyqtSignal, QObject
from mariadb import Cursor
import mariadb

from database.models.mwlink import MwLink
from procedures.utils.helpers import calc_distance

from handlers import config_handler

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
                user=self.settings['user'],
                password=self.settings['pass'],
                host=self.settings['address'],
                port=int(self.settings['port']),
                database=self.settings['db_metadata'],
                connect_timeout=int(int(self.settings['timeout']) / 1000),
                reconnect=True
            )

            self.is_connected = True
            SqlManager.is_error_sent = False

        except mariadb.Error as e:
            if not SqlManager.is_error_sent:
                print(f"Cannot connect to MariaDB Platform: {e}")
                SqlManager.is_error_sent = True
            self.is_connected = False

    def check_connection(self) -> bool:
        """
        Check connection state if it is still active.
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

    def load_metadata(self) -> dict:
        """
        Load metadata of CMLs from MariaDB.
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

                    link = MwLink(ID, address_A + ' <--> ' + address_B, technology_influx, address_A, address_B,
                                  frequency_A, frequency_B, polarization, IP_address_A, IP_address_B, link_length,
                                  latitude_A, longitude_A, latitude_B, longitude_B,
                                  dummy_latitude_A, dummy_longitude_A, dummy_latitude_B, dummy_longitude_B)

                    links[ID] = link

                return links
            else:
                raise mariadb.Error('Connection is not active.')
        except mariadb.Error as e:
            # TODO: exception handling
            print(f"Failed to read data from MariaDB: {e}")
            return {}

    def get_last_realtime(self) -> dict:
        """
        Get parameters of last running realtime calculation from output database.
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
                    realtime_params['start_time'] = started
                    realtime_params['retention'] = retention
                    realtime_params['timestep'] = timestep
                    realtime_params['resolution'] = resolution
                    realtime_params['X_MIN'] = X_MIN
                    realtime_params['X_MAX'] = X_MAX
                    realtime_params['Y_MIN'] = Y_MIN
                    realtime_params['Y_MAX'] = Y_MAX

                return realtime_params
            else:
                raise mariadb.Error('Connection is not active.')
        except mariadb.Error as e:
            # TODO: exception handling
            print(f"Failed to read data from MariaDB: {e}")
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
        """
        try:
            if self.check_connection():
                cursor: Cursor = self.connection.cursor()

                query = f"INSERT INTO {self.settings['db_output']}.realtime_rain_parameters " \
                        "(retention, timestep, resolution, X_MIN, X_MAX, Y_MIN, Y_MAX, X_count, Y_count)" \
                        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"

                x = int((X_MAX - X_MIN) / resolution + 1)
                y = int((Y_MAX - Y_MIN) / resolution + 1)

                cursor.execute(query, (retention, timestep, resolution, X_MIN, X_MAX, Y_MIN, Y_MAX, x, y))
                self.connection.commit()

                # store the ID of the inserted record
                self.realtime_params_id = cursor.lastrowid
            else:
                raise mariadb.Error('Connection is not active.')
        except mariadb.Error as e:
            # TODO: exception handling
            print(f"Failed to insert data into MariaDB: {e}")

    def get_last_raingrid(self) -> dict:
        """
        Get last raingrid from output database.
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
                raise mariadb.Error('Connection is not active.')
        except mariadb.Error as e:
            # TODO: exception handling
            print(f"Failed to read data from MariaDB: {e}")
            return {}

    def insert_raingrid(self, time: datetime, links: list, grid):
        """
        Insert raingrid into output database.
        """
        if self.realtime_params_id == 0:
            raise ValueError('Unknown parameters ID. Realtime parameters has not been set?')

        try:
            if self.check_connection():
                cursor: Cursor = self.connection.cursor()

                query = (f"INSERT INTO {self.settings['db_output']}.realtime_rain_grids "
                         f"(time, parameters, links, grid) VALUES (?, ?, ?, ?);")

                cursor.execute(query, (time, self.realtime_params_id, json.dumps(links), json.dumps(grid)))
                self.connection.commit()
            else:
                raise mariadb.Error('Connection is not active.')
        except mariadb.Error as e:
            # TODO: exception handling
            print(f"Failed to insert data into MariaDB: {e}")

    def wipeout_realtime_tables(self):
        """
        Truncate realtime tables in output database.
        """
        try:
            if self.check_connection():
                cursor: Cursor = self.connection.cursor()

                queries = (
                    f"TRUNCATE TABLE {self.settings['db_output']}.realtime_rain_grids;",
                    f"TRUNCATE TABLE {self.settings['db_output']}.realtime_rain_parameters;"
                )

                for query in queries:
                    cursor.execute(query)
                self.connection.commit()
            else:
                raise mariadb.Error('Connection is not active.')
        except mariadb.Error as e:
            # TODO: exception handling
            print(f"Failed to insert data into MariaDB: {e}")

    def get_wetdry_calibration(self, link_id: int, link_channel: int, time: datetime, night: bool) -> float:
        """
        TODO: currently not used, consider removing together with the table telcorain_calibration_wetdry
        Get wet/dry calibration value for given link, date, and day phase (day/night).
        """
        try:
            if self.check_connection():
                cursor: Cursor = self.connection.cursor()

                query = f"SELECT sd " \
                        f"FROM telcorain_calibration_wetdry " \
                        f"WHERE link_ID = ? AND link_channel = ? AND night = ? " \
                        f"ORDER BY ABS(TIMESTAMPDIFF(SECOND, time, ?)) " \
                        f"LIMIT 1;"

                cursor.execute(query, (int(link_id), int(link_channel), int(night), time))

                sd = 0.0

                for val in cursor:
                    sd = val[0]

                return sd
            else:
                raise mariadb.Error('Connection is not active.')
        except mariadb.Error as e:
            # TODO: exception handling
            print(f"Failed to read data from MariaDB: {e}")
            return 0

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
