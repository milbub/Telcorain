import sqlite3


class MwLink:
    def __init__(self, link_id: int, name: str, tech: str, name_a: str, name_b: str, freq_a: int, freq_b: int,
                 polarization: str, ip_a: str, ip_b: str, distance: float, latitude_a: str, longitude_a: str,
                 latitude_b: str, longitude_b: str, dummy_latitude_a: str, dummy_longitude_a: str,
                 dummy_latitude_b: str, dummy_longitude_b: str):
        self.link_id = link_id
        self.name = name
        self.tech = tech
        self.name_a = name_a
        self.name_b = name_b
        self.freq_a = freq_a
        self.freq_b = freq_b
        self.polarization = polarization
        self.ip_a = ip_a
        self.ip_b = ip_b
        self.distance = distance
        self.latitude_a = latitude_a
        self.longitude_a = longitude_a
        self.latitude_b = latitude_b
        self.longitude_b = longitude_b
        self.dummy_latitude_a = dummy_latitude_a
        self.dummy_longitude_a = dummy_longitude_a
        self.dummy_latitude_b = dummy_latitude_b
        self.dummy_longitude_b = dummy_longitude_b


class SqliteManager:
    def __init__(self):
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
