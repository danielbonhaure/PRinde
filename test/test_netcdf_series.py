import os
from core.lib.utils.extended_collections import DotDict
from core.model.Location import Location
from core.modules.simulations_manager.weather.NetCDFSeriesMaker import NetCDFSeriesMaker

__author__ = 'Federico Schmidt'

import unittest


class TestNetCDFSeries(unittest.TestCase):

    def setUp(self):
        self.series_maker = NetCDFSeriesMaker(system_config={}, max_parallelism=1)
        self.default_varnames = {
            'time': 'time',
            'x': 'x',
            'y': 'y',
            'scen': None
        }
        self.default_dimensions = ['time', 'x', 'y', 'scen']
        if not os.path.exists('test/data/.tmp'):
            os.makedirs('test/data/.tmp')
        self.fake_forecast = DotDict({
            'paths': {'weather_grid_path': 'test/data/.tmp'},
            'configuration': {'netcdf_variables': self.default_varnames, 'grid_resolution': 10}
        })
        self.test_location = DotDict({
            'netcdf_x': 5127500,
            'netcdf_y': 5907500,
            'x_coord': -64.178711,
            'y_coord': -36.910294,
            'weather_station': 1,
            'name': 'Zone 1'
        })

    def test_access_tuple(self):
        dimensions = ['t', 'x_coord', 'y_coord']
        varnames = {
            'time': 't',
            'x': 'x_coord',
            'y': 'y_coord',
            'scen': None
        }
        t1 = self.series_maker.get_access_tuple(dimensions, varnames, x_idx=1, y_idx=1, scen_number=0)
        t2 = self.series_maker.get_access_tuple(self.default_dimensions, self.default_varnames, x_idx=1, y_idx=1, scen_number=0)

        self.assertEqual(t1, t2)
