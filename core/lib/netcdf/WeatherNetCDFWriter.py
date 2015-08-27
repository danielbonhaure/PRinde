# -*- coding: utf-8 -*-
import os
import logging
from core.modules.PreparadorDeSimulaciones.DatabaseWeatherSeries import DatabaseWeatherSeries

__author__ = 'Federico Schmidt'

import csv
from netCDF4 import Dataset as NetCDF
import time
from datetime import datetime
import numpy as np
import copy


class WeatherNetCDFWriter:
    reference_date = "1950-01-01"

    def __init__(self):
        pass

    @staticmethod
    def join_csv_files(dir_list, output_file_path, extract_rainfall=True, forecast_date=None, station_data=None):
        # def join_csv_files(dir_list, output_file_path, extract_rainfall=True):
        """
        Joins a list of CSV files into one NetCDF file that's compatible with pSIMS' format.
        :param dir_list: The list of paths
        :param output_file_path: Output file path, should be absolute.
        :param extract_rainfall: Wether to extract rainfall data from weather series or not.
        :param forecast_date: Only used it extract_rainfall is True.
        :return: A dictionary if extract_rainfall is True, None otherwise.
        """
        proc_start_time = time.time()

        necdf_file_name = "%s_%s.psims.nc" % (station_data['grid_row'], station_data['grid_column'])
        nectdf_file_path = os.path.join(output_file_path, necdf_file_name)

        output_file = NetCDF(nectdf_file_path, 'w')

        csv_files = dir_list

        # If there's only one scenario, we call it '0' internally. Though the NetCDF output file won't have the
        # 'scen' dimension defined. This is needed when we extract rainfall data.
        scen_names = [0]
        if len(csv_files) > 1:
            # If there's more than one file, we extract the scenario name (the year of the climate series).
            scen_names = [DatabaseWeatherSeries.__scen_name__(i) for i in csv_files]

        expected_variables = {'fecha', 'rad', 'tmax', 'tmin', 'prcp'}
        var_units = {
            "rad": "MJm-2",
            "tmax": "C",
            "tmin": "C",
            "prcp": "mm"
        }

        # Define base dimensions, it's contents and units.
        dims = ['latitude', 'longitude', 'time']
        dim_var_contents = [
            [0],
            [0],
            []
        ]
        dims_units = ['degrees_east', 'degrees_north', 'days since %s 00:00:00' % WeatherNetCDFWriter.reference_date]

        rainfall_data = None

        # Add scen dimension in case there is more than one weather file.
        if len(csv_files) > 1:
            dims = dims + ['scen']
            dim_var_contents += [scen_names]
            dims_units += ['Scenarios']

        # Create dimensions.
        for index, dim in enumerate(dims):
            output_file.createDimension(dim)
            dim_var = output_file.createVariable(dim, 'int32', (dim,))
            dim_var.units = dims_units[index]
            dim_var[:] = dim_var_contents[index]

        variables_contents = {}
        time_var_content = []

        # Parse reference_date (str) to date.
        ref_date = datetime.strptime(WeatherNetCDFWriter.reference_date, '%Y-%m-%d')

        # Calculate forecast date as time difference (in days).
        if forecast_date:
            forecast_date = datetime.strptime(forecast_date, '%Y-%m-%d')
            forecast_date = forecast_date - ref_date
            forecast_date = forecast_date.days

        # Loop through CSV weather files.
        for scen_index, f in enumerate(csv_files):
            # Unpack dictionary entry: (key, value).
            scen_name = scen_names[scen_index]
            csv_file = csv.reader(open(f), delimiter='\t')

            csv_variables = []
            csv_content = dict()

            for r_index, row in enumerate(csv_file):
                if r_index == 0:
                    # Header
                    csv_variables = row

                    # Check that the header variables match the expected variables.
                    if len(expected_variables.intersection(csv_variables)) != len(expected_variables):
                        raise RuntimeError("The variables in the CSV file \"%s\" don't match the expected ones (%s)." %
                                           (csv_files[scen_index], expected_variables))

                    for column in row:
                        csv_content[column] = []

                else:
                    for i, value in enumerate(row):
                        var_name = csv_variables[i]
                        csv_content[var_name].append(value)

            csv_content['time'] = csv_content['fecha']
            del csv_content['fecha']

            # Calculate time diff in days for each date.
            for i, day in enumerate(csv_content['time']):
                day = datetime.strptime(day, '%Y-%m-%d')
                delta = day - ref_date
                csv_content['time'][i] = delta.days

            # Initialize the content of the time variable for it to be written once we finish
            # writting the other variables.
            if len(time_var_content) == 0:
                time_var_content = copy.deepcopy(csv_content['time'])
            else:
                # If it's already initialized, check that every CSV file has data for the same days.
                if time_var_content != csv_content['time']:
                    raise RuntimeError("Dates do not match between CSV files.")

            # Delete this variable to avoid trying to write it to the NetCDF file.
            del csv_content['time']

            # Loop through each variable in the CSV header.
            for var_name in csv_content.keys():
                if var_name not in expected_variables:
                    continue

                if var_name in variables_contents:
                    var_array = variables_contents[var_name]
                else:
                    # Initialize this variable.
                    shape = (len(csv_content[var_name]), 1, 1)
                    if len(csv_files) > 1:
                        shape = (len(csv_files),) + shape

                    var_array = np.empty(shape=shape)
                    var_array.fill(-99)
                    variables_contents[var_name] = var_array

                # Write variable content.
                if len(csv_files) > 1:
                    # The file index will be the scenario number.
                    var_array[scen_index, 0:len(csv_content[var_name]), 0, 0] = csv_content[var_name]
                else:
                    var_array[:, 0, 0] = csv_content[var_name]

        # Create the dimensions tuple to create variables in the NetCDF file.
        dims = ('time', 'latitude', 'longitude')
        if len(csv_files) > 1:
            dims = ('scen',) + dims

        start_time = time.time()
        # Write variables to the NetCDF file.
        for var_name in variables_contents:
            netcdf_var = output_file.createVariable(var_name, 'float32', dims, fill_value=-99)
            netcdf_var.units = var_units[var_name]

            netcdf_var[:] = variables_contents[var_name]

        # Check if we need to extract rainfall data.
        if extract_rainfall:
            rainfall_data = dict()
            rain_variable = variables_contents['prcp']
            # Convert time variable to Numpy array, otherwise we can't use array indexes.
            time_var_content = np.array(time_var_content)

            # Again, check if there's more than one climate serie.
            if len(csv_files) > 1:
                # Extract rainfall data until the date of forecast.
                pre_forecast_time = time_var_content[time_var_content <= forecast_date]
                rain = rain_variable[0, 0:len(pre_forecast_time), 0, 0]
                rainy_days = np.where(rain > 0)[0]

                rainfall_data['0'] = {
                    'dates': pre_forecast_time[rainy_days].tolist(),
                    'values': rain[rainy_days].tolist()
                }

                # Extract rainfall data for each scenario after the forecast date.
                for i, year in enumerate(scen_names):
                    post_forecast_time = time_var_content[time_var_content > forecast_date]
                    post_forecast_start = len(time_var_content) - len(post_forecast_time)
                    rain = rain_variable[i, post_forecast_start:, 0, 0]

                    rainy_days = np.where(rain > 0)[0]
                    rainfall_data[str(year)] = {
                        'dates': post_forecast_time[rainy_days].tolist(),
                        'values': rain[rainy_days].tolist()
                    }
            else:
                rain = rain_variable[:, 0, 0]
                rainy_days = np.where(rain > 0)[0]

                rainfall_data['0'] = {
                    'dates': time_var_content[rainy_days].tolist(),
                    'values': rain[rainy_days].tolist()
                }

        time_var = output_file.variables['time']
        time_var[:] = time_var_content

        output_file.close()
        logging.getLogger().debug('Write NetCDF file: %f.' % (time.time() - start_time))
        logging.getLogger().debug("NetCDF file created: '%s'. Time: %s." %
                                  (output_file_path, (time.time() - proc_start_time)))

        result = os.path.exists(nectdf_file_path)
        return result, rainfall_data
