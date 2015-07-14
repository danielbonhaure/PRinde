import abc

__author__ = 'Federico Schmidt'

import threading
from core.lib.netcdf.WeatherNetCDFWriter import WeatherNetCDFWriter
from core.modules.PreparadorDeSimulaciones.WeatherSeriesMaker import WeatherSeriesMaker
from core.lib.geo.grid import latlon_to_grid
from core.lib.io.file import listdir_fullpath, create_folder_with_permissions
import os.path
import shutil
import csv


class DatabaseWeatherSeries(WeatherSeriesMaker):

    def __init__(self, system_config, max_paralellism, weather_writer=WeatherNetCDFWriter):
        super(DatabaseWeatherSeries, self).__init__(system_config, max_paralellism)
        self.max_paralellism = max_paralellism
        self.weather_writer = weather_writer
        self.concurrency_lock = threading.BoundedSemaphore(self.max_paralellism)

    def create_series(self, omm_id, forecast, extract_rainfall=True):
        with self.concurrency_lock:
            print("Running for omm_id = %s" % omm_id)
            output_path = os.path.join(forecast.paths.wth_csv_read, str(omm_id))
            create_folder_with_permissions(output_path)

            # Create the weather series in CSV format.
            self.create_from_db(omm_id, forecast)

            # List the directory where the CSV files with the weather information are.
            dir_list = listdir_fullpath(output_path, onlyFiles=True, filter=(lambda x: x.endswith('csv')))

            # Build the main file path (the file with the weather station's information).
            main_file = os.path.join(output_path, ('_' + str(omm_id) + '.csv'))

            if main_file not in dir_list:
                raise RuntimeError('Missing station information for a weather serie (omm_id = %s).' % omm_id)

            station_info = self.read_station_info(open(main_file))
            forecast.weather_stations[omm_id] = station_info

            weather_grid = forecast.paths['weather_grid_path']

            grid_row_folder = os.path.join(weather_grid, str(station_info['grid_row']))
            if not os.path.exists(grid_row_folder):
                create_folder_with_permissions(grid_row_folder)

            grid_column_folder = os.path.join(grid_row_folder, str(station_info['grid_column']))
            if not os.path.exists(grid_column_folder):
                create_folder_with_permissions(grid_column_folder)

            necdf_file_name = "%s_%s.psims.nc" % (station_info['grid_row'], station_info['grid_column'])
            nectdf_file_path = os.path.join(grid_column_folder, necdf_file_name)

            dir_list.remove(main_file)

            result_ok, rainfall_data = self.weather_writer.join_csv_files(dir_list, grid_column_folder,
                                                                          extract_rainfall=extract_rainfall,
                                                                          forecast_date=forecast.forecast_date,
                                                                          station_data=station_info)

            # result_ok, rainfall_data = WeatherNetCDFWriter.join_csv_files(dir_list, nectdf_file_path,
            #                                                               extract_rainfall=extract_rainfall,
            #                                                               forecast_date=forecast.forecast_date)

            if extract_rainfall:
                forecast.rainfall[str(omm_id)] = rainfall_data

            if result_ok:
                shutil.rmtree(output_path)
                forecast.weather_stations[omm_id]['weather_path'] = nectdf_file_path
            else:
                raise RuntimeError('Couldn\'t create NetCDF weather file "%s".' % nectdf_file_path)

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

    @abc.abstractmethod
    def create_from_db(self, omm_id, forecast):
        pass
