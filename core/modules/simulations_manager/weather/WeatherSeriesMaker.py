import abc
from core.lib.geo.grid import latlon_to_grid

__author__ = 'Federico Schmidt'


class WeatherSeriesMaker(metaclass=abc.ABCMeta):
    def __init__(self, system_config, max_paralellism):
        self.system_config = system_config

    @abc.abstractmethod
    def create_series(self, location, forecast, extract_rainfall=False):
        pass

    @staticmethod
    def validate_location(location_yaml, forecast, system_config):
        """
        This method should perform a check on a location yaml and fill any missing fields or correct any field with
        invalid values.
        :param location_yaml: The location YAML to check and fill.
        :param forecast: The forecast to which the location belongs.
        :param system_config: The system config.
        :raise RuntimeError: if the location has any error that can't be corrected.
        :returns The location YAML after being checked and filled (if necessary).
        """
        return location_yaml

    @staticmethod
    def expand_station_info(station_info, forecast_grid_resolution=30):
        keys = list(station_info.keys())

        if 'grid_row' not in keys or 'grid_column' not in keys:
            cell = latlon_to_grid(lat_dec=float(station_info.get('coord_y')),
                                  lon_dec=float(station_info.get('coord_x')),
                                  resolution=forecast_grid_resolution)
            station_info['grid_row'] = cell.row
            station_info['grid_column'] = cell.column
        return station_info
