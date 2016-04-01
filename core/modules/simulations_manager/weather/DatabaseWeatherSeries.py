import abc
import re
import threading
import os.path

from core.modules.simulations_manager.weather.WeatherSeriesMaker import WeatherSeriesMaker
from core.lib.geo.grid import latlon_to_grid
from core.lib.io.file import listdir_fullpath, create_folder_with_permissions

__author__ = 'Federico Schmidt'


class DatabaseWeatherSeries(WeatherSeriesMaker):
    name_re = re.compile('^[0-9]+ - ([0-9]+)\.csv')

    def __init__(self, system_config, max_parallelism, weather_writer):
        super(DatabaseWeatherSeries, self).__init__(system_config, max_parallelism)
        self.max_paralellism = max_parallelism
        self.weather_writer = weather_writer
        self.concurrency_lock = threading.BoundedSemaphore(self.max_paralellism)

    def create_series(self, location, forecast, extract_rainfall=True):
        with self.concurrency_lock:
            omm_id = location['weather_station']

            station_info = self.expand_station_info(location)

            forecast.weather_stations[omm_id] = station_info

            weather_grid = forecast.paths['weather_grid_path']

            grid_row_folder = os.path.join(weather_grid, '_%03d' % station_info['grid_row'])
            if not os.path.exists(grid_row_folder):
                create_folder_with_permissions(grid_row_folder)

            grid_column_folder = os.path.join(grid_row_folder, '_%03d' % station_info['grid_column'])
            if not os.path.exists(grid_column_folder):
                create_folder_with_permissions(grid_column_folder)

            rainfall_dict = dict()
            scen_names = []
            scen_index = 0
            for scen_year, scen_weather in self.create_from_db(location, forecast):
                variables_dict = self.weather_writer.write_wth_file(scen_index, scen_weather, grid_column_folder,
                                                                    location)
                if extract_rainfall:
                    self.weather_writer.extract_rainfall(rainfall_dict, variables_dict, forecast.forecast_date,
                                                         scen_year)
                scen_names.append(scen_year)
                scen_index += 1

            if extract_rainfall:
                forecast.rainfall[str(omm_id)] = rainfall_dict

            forecast.weather_stations[omm_id]['weather_path'] = grid_column_folder
            forecast.weather_stations[omm_id]['num_scenarios'] = len(scen_names)
            forecast.weather_stations[omm_id]['scen_names'] = scen_names

    def expand_station_info(self, station_info):
        keys = station_info.keys()

        if 'grid_row' not in keys or 'grid_column' not in keys:
            cell = latlon_to_grid(lat_dec=float(station_info.get('coord_y')),
                                  lon_dec=float(station_info.get('coord_x')),
                                  resolution=self.system_config.grid_resolution)
            station_info['grid_row'] = cell.row
            station_info['grid_column'] = cell.column
        return station_info

    @abc.abstractmethod
    def create_from_db(self, location, forecast):
        return iter([])

