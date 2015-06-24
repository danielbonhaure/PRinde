from datetime import datetime
from core.lib.io.file import fileNameWithoutExtension
from core.lib.utils.DotDict import DotDict

__author__ = 'Federico Schmidt'


class Forecast(DotDict):

    def __init__(self, yaml_file):
        super(Forecast, self).__init__()

        self.init_from_yaml(yaml_file)

        if 'weather_series' not in self.configuration:
            self.configuration.weather_series = 'combined'

        # Alias
        self.paths = self.configuration.paths
        self.simulations = []

    def init_from_yaml(self, yaml):
        if 'name' not in yaml:
            yaml['name'] = fileNameWithoutExtension(yaml['file_name'])

        if 'forecast_date' not in yaml:
            # If there's no forecast date defined, we assume that the current date is the forecast date.
            yaml['forecast_date'] = "now"

        # Keys that belong to simulation objects and must be deleted from a Forecast object.
        simulation_keys = ['initial_conditions', 'agronomic_management', 'site_characteristics']
        for key in simulation_keys:
            if key in yaml:
                del yaml[key]

        self.__dict__.update(DotDict(yaml))

    @property
    def start_date(self):
        """
        Calculates the start date of this forecast campaign.
        :rtype : datetime
        """
        f_date = datetime.strptime(self.forecast_date, '%Y-%m-%d')

        if f_date.month < 5:
            return f_date.replace(day=1, month=1, year=f_date.year - 1)
        return f_date.replace(day=1, month=1)

    @property
    def end_date(self):
        """
        Calculates the end date of this forecast campaign.
        :rtype : datetime
        """
        f_date = datetime.strptime(self.forecast_date, '%Y-%m-%d')

        if f_date.month < 5:
            return f_date.replace(day=31, month=12)
        return f_date.replace(day=31, month=12, year=f_date.year + 1)

    def __getitem__(self, key):
        val = self.__dict__[key]
        # If we're accessing the forecast date and it should be the actual date, we replace it.
        if key == 'forecast_date' and val == 'now':
            return datetime.now().strftime('%Y-%m-%d')
        return val

    def __getattribute__(self, item):
        val = object.__getattribute__(self, item)
        # If we're accessing the forecast date and it should be the actual date, we replace it.
        if item == 'forecast_date' and val == 'now':
            return datetime.now().strftime('%Y-%m-%d')
        return val
