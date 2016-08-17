from core.modules.simulations_manager.weather.DatabaseWeatherSeries import DatabaseWeatherSeries
from core.modules.simulations_manager.weather.HistoricalSeriesMaker import HistoricalSeriesMaker
from core.modules.simulations_manager.weather.CombinedSeriesMaker import CombinedSeriesMaker
from core.modules.simulations_manager.weather.NetCDFSeriesMaker import NetCDFSeriesMaker
import unittest

__author__ = 'Federico Schmidt'


class TestWeatherMakerSublcass(unittest.TestCase):

    def test_is_subclass(self):
        self.assertTrue(issubclass(CombinedSeriesMaker, DatabaseWeatherSeries))
        self.assertTrue(issubclass(HistoricalSeriesMaker, DatabaseWeatherSeries))
        self.assertFalse(issubclass(NetCDFSeriesMaker, DatabaseWeatherSeries))

    def test_instance_is_subclass(self):
        combined = CombinedSeriesMaker({}, max_parallelism=1)
        historical = HistoricalSeriesMaker({}, max_parallelism=1)
        netcdf = NetCDFSeriesMaker({}, max_parallelism=1)
        self.assertTrue(issubclass(combined.__class__, DatabaseWeatherSeries))
        self.assertTrue(issubclass(historical.__class__, DatabaseWeatherSeries))
        self.assertFalse(issubclass(netcdf.__class__, DatabaseWeatherSeries))