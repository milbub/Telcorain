class MwLink:
    def __init__(self, link_id: int, name: str, tech: str, name_a: str, name_b: str, freq_a: int, freq_b: int,
                 polarization: str, ip_a: str, ip_b: str, distance: float, latitude_a: float, longitude_a: float,
                 latitude_b: float, longitude_b: float, dummy_latitude_a: float, dummy_longitude_a: float,
                 dummy_latitude_b: float, dummy_longitude_b: float):
        self.link_id = link_id
        self.name = name
        self.tech = tech
        self.name_a = name_a
        self.name_b = name_b
        self.freq_a = freq_a
        self.freq_b = freq_b

        # since we can't handle cross polarization yet, let's consider them to have vertical polarization temporarily
        if polarization == "X":
            polarization = "V"

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

