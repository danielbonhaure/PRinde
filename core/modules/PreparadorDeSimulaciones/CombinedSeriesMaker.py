import logging
import threading
from core.lib.netcdf.WeatherNetCDFWriter import WeatherNetCDFWriter
from core.modules.PreparadorDeSimulaciones.WeatherSeriesMaker import WeatherSeriesMaker
from core.lib.geo.grid import latlon_to_grid
from core.lib.io.file import listdir_fullpath, create_folder_with_permissions
import os.path
import shutil
import csv
import time


__author__ = 'Federico Schmidt'


class CombinedSeriesMaker(WeatherSeriesMaker):

    def __init__(self, system_config, max_paralellism):
        super(CombinedSeriesMaker, self).__init__(system_config, max_paralellism)
        self.ncdf_writer = WeatherNetCDFWriter()
        self.max_paralellism = max_paralellism
        print("Max parallel jobs: %s." % self.max_paralellism)
        self.concurrency_lock = threading.BoundedSemaphore(self.max_paralellism)

    def create_series(self, omm_id, forecast):
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
            forecast.simulations[omm_id]['station_info'] = station_info

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

            self.ncdf_writer.join_csv_files(dir_list, nectdf_file_path)

            if os.path.exists(nectdf_file_path):
                shutil.rmtree(output_path)
            else:
                raise RuntimeError('Couldn\'t create NetCDF weather file "%s".' % nectdf_file_path)

    def create_from_db(self, omm_id, forecast):
        wth_output = os.path.join(forecast.paths.wth_csv_export, str(omm_id))

        forecast_date = forecast.forecast_date
        start_date = forecast['start_date'].strftime('%Y-%m-%d')
        end_date = forecast['end_date'].strftime('%Y-%m-%d')

        wth_db_connection = self.system_config.database['weather_db']
        cursor = wth_db_connection.cursor()

        start_time = time.time()
        cursor.execute("SELECT pr_create_campaigns(%s, %s, %s, %s, %s)",
                       (omm_id, start_date, forecast_date, end_date,
                        wth_output))
        logging.getLogger("main").debug("Station: %s. Date: %s. Time: %s." % (omm_id, forecast_date, (time.time() - start_time)))

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
        return(info)