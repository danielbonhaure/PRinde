import abc
import logging
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

            station_info = WeatherSeriesMaker.expand_station_info(location, forecast.configuration.grid_resolution)

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

    @abc.abstractmethod
    def create_from_db(self, location, forecast):
        return iter([])

    @staticmethod
    def validate_location(location_yaml, forecast, system_config):
        weather_db_connection = system_config.database['weather_db']
        name = location_yaml['name']

        if 'weather_station' not in location_yaml or len(str(location_yaml['weather_station'])) == 0:
            # Perform weather station lookup.
            if 'coord_x' not in location_yaml or 'coord_y' not in location_yaml:
                raise RuntimeError('No weather station ID or coordinates provided for location "%s".' % name)

            try:
                coord_x = float(location_yaml['coord_x'])
                coord_y = float(location_yaml['coord_y'])
            except Exception:
                raise RuntimeError('Failed to parse coordinates to float for location "%s".' % name)

            nearest_station_query = """
            SELECT ROUND(((point(e.lon_dec, e.lat_dec) <@> point(%s, %s)) * 1.61)::numeric, 2) distance_km,
                   e.omm_id, e.nombre
            FROM estacion e
            ORDER BY 1 ASC
            LIMIT 1
            """

            cursor = weather_db_connection.cursor()
            cursor.execute(nearest_station_query, (coord_x, coord_y))

            result = cursor.fetchone()
            logging.debug('Found station "%s" at %s kilometers from (%s, %s) for location "%s".' % (
                result[2], result[0], coord_y, coord_x, name
            ))
            location_yaml['weather_station'] = result[1]
        else:
            try:
                location_yaml['weather_station'] = int(location_yaml['weather_station'])
            except:
                raise RuntimeError('Failed to parse weather_station field to int for location "%s".' % name)

            find_station = """
            SELECT e.nombre, e.lat_dec, e.lon_dec FROM estacion e WHERE e.omm_id = %s
            """

            cursor = weather_db_connection.cursor()
            cursor.execute(find_station, (location_yaml['weather_station'], ))

            result = cursor.fetchone()
            if not result:
                raise RuntimeError('No weather station found with id = %s for location "%s".' %
                                   (location_yaml['weather_station'], name))

            if 'name' not in location_yaml:
                # Create a name based on the weather station that's being used.
                location_yaml['name'] = result[0]

            if 'coord_x' not in location_yaml or len(location_yaml['coord_x']) == 0:
                # Set x coordinate (longitude).
                location_yaml['coord_x'] = '%s' % result[2]

            if 'coord_y' not in location_yaml or len(location_yaml['coord_y']) == 0:
                # Set y coordinate (latitude).
                location_yaml['coord_y'] = '%s' % result[1]
        return location_yaml

