import abc

__author__ = 'Federico Schmidt'


class WeatherSeriesMaker:
    __metaclass__ = abc.ABCMeta

    def __init__(self, system_config, max_paralellism):
        self.system_config = system_config

    @abc.abstractmethod
    def create_series(self, location, forecast, extract_rainfall=False):
        pass