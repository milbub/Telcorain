from math import sin, cos, sqrt, atan2, radians

import numpy as np


def calc_distance(lat_A: float, long_A: float, lat_B: float, long_B: float) -> float:
    """
    Calculate distance between two points on Earth.

    :param lat_A: latitude of point A in decimal degrees
    :param long_A: longitude of point A in decimal degrees
    :param lat_B: latitude of point B in decimal degrees
    :param long_B: longitude of point B in decimal degrees
    :return: distance in kilometers
    """
    # Approximate radius of earth in km
    r = 6373.0

    lat_A = radians(lat_A)
    long_A = radians(long_A)
    lat_B = radians(lat_B)
    long_B = radians(long_B)

    dlon = long_B - long_A
    dlat = lat_B - lat_A

    a = sin(dlat / 2) ** 2 + cos(lat_A) * cos(lat_B) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return r * c


def dt64_to_unixtime(dt64: np.datetime64) -> int:
    """
    Convert numpy datetime64 to Unix timestamp.

    :param dt64: numpy datetime64
    :return: number of seconds since Unix epoch
    """
    unix_epoch = np.datetime64(0, 's')
    s = np.timedelta64(1, 's')
    return int((dt64 - unix_epoch) / s)
