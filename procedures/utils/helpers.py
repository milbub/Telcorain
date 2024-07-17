from math import sin, cos, sqrt, atan2, radians

import numpy as np
from shapely.geometry import Point
from shapely.prepared import PreparedGeometry


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


def mask_grid(
        data_grid: np.ndarray,
        x_grid: np.ndarray,
        y_grid: np.ndarray,
        prepared_polygons: list[PreparedGeometry]
) -> np.ndarray:
    """
    Mask the 2D data grid with polygons. If a point is not within any of the polygons, it is set to NaN.

    :param data_grid: 2D ndarray data grid to be masked
    :param x_grid: 2D ndarray of x coordinates
    :param y_grid: 2D ndarray of y coordinates
    :param prepared_polygons: list of prepared polygons
    :return: masked 2D ndarray with NaN values outside the polygons
    """
    mask = np.vectorize(lambda lon, lat: any(polygon.contains(Point(lon, lat)) for polygon in prepared_polygons))
    within_polygons = mask(x_grid, y_grid)
    data_grid[~within_polygons] = np.nan
    return data_grid
