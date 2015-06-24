# -*- coding: utf-8 -*-
import logging

__author__ = 'Federico Schmidt'

import csv
from netCDF4 import Dataset as nc
import time
from datetime import datetime
import numpy as np
import copy


class WeatherNetCDFWriter:

    def __init__(self):
        pass

    @staticmethod
    def join_csv_files(dir_list, output_file_path):
        proc_start_time = time.time()
        output_file = nc(output_file_path, 'w')

        csv_files = dir_list
        csv_readers = [csv.reader(open(f), delimiter='\t') for f in csv_files]

        expected_variables = {'fecha', 'rad', 'tmax', 'tmin', 'prcp'}
        var_units = {
            "rad": "MJm-2",
            "tmax": "C",
            "tmin": "C",
            "prcp": "mm"
        }

        ref_date = datetime.strptime("1950-01-01", '%Y-%m-%d')

        # Define base dimensions, it's contents and units.
        dims = ['latitude', 'longitude', 'time']
        dim_var_contents = [
            [0],
            [0],
            []
        ]
        dims_units = ['degrees_east', 'degrees_north', 'days since 1950-01-01 00:00:00']

        # Add scen dimension in case there is more than one weather file.
        if len(csv_files) > 1:
            dims = dims + ['scen']
            dim_var_contents += [[i for i in range(0, len(csv_files))]]
            dims_units += ['Scenarios']

        # Create dimensions.
        for index, dim in enumerate(dims):
            new_dim = output_file.createDimension(dim)
            dim_var = output_file.createVariable(dim, 'int32', (dim,))
            dim_var.units = dims_units[index]
            dim_var[:] = dim_var_contents[index]

        variables_contents = {}
        time_var_content = []

        # Loop through CSV weather files.
        for scen_index, csv_file in enumerate(csv_readers):
            #print('\r%02d/%02d' % (scen_index+1, len(csv_readers))),
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

        #print('')
        time_var = output_file.variables['time']
        time_var[:] = time_var_content

        output_file.close()
        logging.getLogger("main").debug('Write NetCDF file: %f.' % (time.time() - start_time))
        logging.getLogger("main").debug("NetCDF file created: '%s'. Time: %s." % (output_file_path, (time.time() - proc_start_time)))