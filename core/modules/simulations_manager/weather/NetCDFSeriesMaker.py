import itertools
from core.lib.dssat.DSSATWthWriter import DSSATWthWriter

__author__ = 'Federico Schmidt'

import threading
import os.path
import _strptime
from datetime import datetime, timedelta
from netCDF4 import Dataset
from core.modules.simulations_manager.weather.WeatherSeriesMaker import WeatherSeriesMaker
from core.lib.io.file import create_folder_with_permissions


class NetCDFSeriesMaker(WeatherSeriesMaker):

    def __init__(self, system_config, max_parallelism, weather_writer=None):
        super(NetCDFSeriesMaker, self).__init__(system_config, max_parallelism)
        if not weather_writer:
            weather_writer = DSSATWthWriter
        self.max_paralellism = max_parallelism
        self.weather_writer = weather_writer
        self.concurrency_lock = threading.BoundedSemaphore(self.max_paralellism)

    def create_series(self, location, forecast, extract_rainfall=True):
        with self.concurrency_lock:
            zone_id = location['weather_station']

            station_info = WeatherSeriesMaker.expand_station_info(location, forecast.configuration.grid_resolution)

            forecast.weather_stations[zone_id] = station_info

            weather_grid = forecast.paths['weather_grid_path']

            grid_row_folder = os.path.join(weather_grid, '_%03d' % station_info['grid_row'])
            if not os.path.exists(grid_row_folder):
                create_folder_with_permissions(grid_row_folder)

            grid_column_folder = os.path.join(grid_row_folder, '_%03d' % station_info['grid_column'])
            if not os.path.exists(grid_column_folder):
                create_folder_with_permissions(grid_column_folder)

            nc = Dataset(forecast.configuration.netcdf_source, mode='r')

            varnames = {
                'x': forecast.configuration.netcdf_variables.get('coord_x', 'x'),
                'y': forecast.configuration.netcdf_variables.get('coord_y', 'y'),
                'time': forecast.configuration.netcdf_variables.get('time', 'time'),
                'scen': forecast.configuration.netcdf_variables.get('scenario', None),
                'tx': forecast.configuration.netcdf_variables.get('tx', 'tx'),
                'tn': forecast.configuration.netcdf_variables.get('tn', 'tn'),
                'prcp': forecast.configuration.netcdf_variables.get('prcp', 'prcp'),
                'srad': forecast.configuration.netcdf_variables.get('srad', 'srad')
            }

            if 'scenario' in forecast.configuration.netcdf_variables:
                varnames['scen'] = forecast.configuration.netcdf_variables['scenario']

            x_proj = nc.variables[varnames['x']][:].tolist()
            y_proj = nc.variables[varnames['y']][:].tolist()

            ref_date = datetime.strptime(nc.variables[varnames['time']].units, 'Days since %Y-%m-%d')

            fecha = [ref_date + timedelta(days=d) for d in nc.variables[varnames['time']]]

            scenarios = [0]
            # Creating a scenario dimension in the NetCDF is optional.
            if varnames['scen'] and len(varnames['scen']) > 0:
                scenarios = nc.variables[varnames['scen']][:].tolist()
            else:
                # If it's an empty variable name, coerce it to None.
                varnames['scen'] = None

            x_idx = x_proj.index(float(location['netcdf_x']))
            y_idx = y_proj.index(float(location['netcdf_y']))

            rainfall_dict = dict()
            scen_names = []

            for scen_index, scenario in enumerate(scenarios):
                clim = {}

                for climate_variable in ['tx', 'tn', 'prcp', 'srad']:
                    access_t = self.get_access_tuple(nc.variables[varnames[climate_variable]].dimensions,
                                                     varnames, x_idx, y_idx, scen_index)
                    clim[climate_variable] = nc.variables[varnames[climate_variable]][access_t]

                rows = zip(fecha, clim['tx'], clim['tn'], clim['prcp'], clim['srad'])
                colnames = [tuple(['fecha', 'tmax', 'tmin', 'prcp', 'rad'])]
                scen_weather = itertools.chain(colnames, rows)

                variables_dict = self.weather_writer.write_wth_file(scen_index, scen_weather, grid_column_folder,
                                                                    location)
                if extract_rainfall:
                    self.weather_writer.extract_rainfall(rainfall_dict, variables_dict, forecast.forecast_date,
                                                         scen_index)
                scen_names.append(scenario)

            if extract_rainfall:
                forecast.rainfall[str(zone_id)] = rainfall_dict

            forecast.weather_stations[zone_id]['weather_path'] = grid_column_folder
            forecast.weather_stations[zone_id]['num_scenarios'] = len(scen_names)
            forecast.weather_stations[zone_id]['scen_names'] = scen_names

    @staticmethod
    def get_access_tuple(var_dimensions, varnames, x_idx, y_idx, scen_number):
        time_var_idx = var_dimensions.index(varnames['time'])
        x_var_idx = var_dimensions.index(varnames['x'])
        y_var_idx = var_dimensions.index(varnames['y'])

        access_tuple = [None] * 3
        if varnames['scen'] is not None:
            access_tuple = [None] * 4
            access_tuple[var_dimensions.index(varnames['scen'])] = scen_number

        access_tuple[x_var_idx] = x_idx
        access_tuple[y_var_idx] = y_idx
        access_tuple[time_var_idx] = Ellipsis

        return tuple(access_tuple)

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
        if 'netcdf_info' not in forecast.configuration:
            if not os.path.exists(forecast.configuration.netcdf_source):
                raise RuntimeError('NetCDF file with path "%s" does not exist.' % forecast.configuration.netcdf_source)

            # nc = Dataset(forecast.configuration.netcdf_source, mode='r')
            #
            # varnames = {
            #     'x': forecast.configuration.netcdf_variables.get('coord_x', 'x'),
            #     'y': forecast.configuration.netcdf_variables.get('coord_y', 'y'),
            #     'time': forecast.configuration.netcdf_variables.get('time', 'time'),
            #     'scen': forecast.configuration.netcdf_variables.get('scenario', None),
            #     'tx': forecast.configuration.netcdf_variables.get('tx', 'tx'),
            #     'tn': forecast.configuration.netcdf_variables.get('tn', 'tn'),
            #     'prcp': forecast.configuration.netcdf_variables.get('prcp', 'prcp'),
            #     'srad': forecast.configuration.netcdf_variables.get('srad', 'srad')
            # }
            #
            forecast.configuration['netcdf_info'] = {
                'exists': True
            }

        if 'netcdf_x' not in location_yaml:
            location_yaml['netcdf_x'] = location_yaml['coord_x']
        if 'netcdf_y' not in location_yaml:
            location_yaml['netcdf_y'] = location_yaml['coord_y']
        return location_yaml
