import abc
import re
import threading
import os.path
import shutil
import csv

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

    def create_series(self, omm_id, forecast, extract_rainfall=True):
        with self.concurrency_lock:
            output_path = os.path.join(forecast.paths.wth_csv_read, str(omm_id))
            create_folder_with_permissions(output_path)

            # Create the weather series in CSV format.
            self.create_from_db(omm_id, forecast)

            # List the directory where the CSV files with the weather information are.
            dir_list = sorted(listdir_fullpath(output_path, onlyFiles=True, filter=(lambda x: x.endswith('csv'))))

            # Build the main file path (the file with the weather station's information).
            main_file = os.path.join(output_path, ('_' + str(omm_id) + '.csv'))

            if main_file not in dir_list:
                raise RuntimeError('Missing station information for a weather serie (omm_id = %s).' % omm_id)

            station_info = self.read_station_info(open(main_file))
            forecast.weather_stations[omm_id] = station_info

            weather_grid = forecast.paths['weather_grid_path']

            grid_row_folder = os.path.join(weather_grid, '_%03d' % station_info['grid_row'])
            if not os.path.exists(grid_row_folder):
                create_folder_with_permissions(grid_row_folder)

            grid_column_folder = os.path.join(grid_row_folder, '_%03d' % station_info['grid_column'])
            if not os.path.exists(grid_column_folder):
                create_folder_with_permissions(grid_column_folder)

            dir_list.remove(main_file)

            result_ok, rainfall_data = self.weather_writer.join_csv_files(dir_list, grid_column_folder,
                                                                          extract_rainfall=extract_rainfall,
                                                                          forecast_date=forecast.forecast_date,
                                                                          station_data=station_info)

            if extract_rainfall:
                forecast.rainfall[str(omm_id)] = rainfall_data

            if result_ok:
                shutil.rmtree(output_path)
                forecast.weather_stations[omm_id]['weather_path'] = grid_column_folder
                forecast.weather_stations[omm_id]['num_scenarios'] = len(dir_list)
                forecast.weather_stations[omm_id]['scen_names'] = [DatabaseWeatherSeries.__scen_name__(csv_file) for
                                                                   csv_file in dir_list]
            else:
                raise RuntimeError('Couldn\'t create weather file(s) in folder "%s".' % grid_column_folder)

    def read_station_info(self, csv_file):
        csv_file = csv.reader(csv_file, delimiter='\t')

        header = csv_file.next()
        row = csv_file.next()

        info = dict()

        for index, item in enumerate(header):
            info[item] = row[index]

        keys = info.keys()

        if 'lat_dec' in keys and 'lon_dec' in keys:
            cell = latlon_to_grid(lat_dec=float(info.get('lat_dec')),
                                  lon_dec=float(info.get('lon_dec')),
                                  resolution=self.system_config.grid_resolution)
            info['grid_row'] = cell.row
            info['grid_column'] = cell.column
        return info

    @staticmethod
    def __scen_name__(csv_filename):
        str_year = DatabaseWeatherSeries.name_re.match(os.path.basename(csv_filename)).groups()[0]
        return int(str_year)

    @abc.abstractmethod
    def create_from_db(self, omm_id, forecast):
        pass
