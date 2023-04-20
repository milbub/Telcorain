import mariadb
import sqlite3
from database.mwlink_model import MwLink


class SqlManager:
    def __init__(self, config_man):
        # TODO: load file name from config
        self.connection = sqlite3.connect('telcolinks.db')

    def load_all(self) -> dict:
        try:
            cursor = self.connection.cursor()
            query = "SELECT id, name, tech, nameA, nameB, freqA, freqB, polarization, " \
                    "ipA, ipB, distance, latA, longA, latB, longB, dummy_latA, dummy_longA, dummy_latB, dummy_longB " \
                    "from MwLink"
            cursor.execute(query)

            records = cursor.fetchall()

            links = {}
            for row in records:
                link = MwLink(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7][0].upper(),
                              row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17],
                              row[18])
                links[link.link_id] = link

            cursor.close()
            return links

        except sqlite3.Error as error:
            # TODO: exception handling
            print("Failed to read data from SQLite file: ", error, flush=True)
