__author__ = 'Federico Schmidt'
from collections import namedtuple
import math

def latlon_to_grid(lat_dec, lon_dec, resolution=30):
    """
    Translates latitude and longitude (in decimal degrees) to a row and column number
    of a grid defined by the resolution parameter (in arcminutes).
    :param lat_dec: Latitude in decimal degrees.
    :param lon_dec: Longitude in decimal degrees.
    :param resolution: Grid's difference in arcminutes between two cells/points.
    :return: Named tuple. Point(row, column)
    """
    point = namedtuple("Point", ["row", "column"])
    point.row = math.ceil((90 - lat_dec) * (60 / resolution))
    point.column = math.ceil((180 + lon_dec) * (60 / resolution))
    return point