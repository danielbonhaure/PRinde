import copy
from datetime import datetime, timedelta
from xxhash import xxh64

from core.lib.io.file import filename_without_ext
from core.lib.utils.extended_collections import DotDict
from core.modules.simulations_manager.psims.WeatherNetCDFWriter import WeatherNetCDFWriter

__author__ = 'Federico Schmidt'


class Forecast(DotDict):

    def __init__(self, yaml_file, simulations):
        super(Forecast, self).__init__()

        self.init_from_yaml(yaml_file)

        if 'weather_series' not in self.configuration:
            self.configuration.weather_series = 'combined'

        # Alias
        self.paths = self.configuration.paths
        self.simulations = simulations
        self.campaign_first_month = self.configuration.get('campaign_first_month', 5)

        hasher = xxh64()

        _sim_ids = []
        for loc_key, simulations in simulations.items():
            for sim in simulations:
                _sim_ids.append(sim.reference_id)

        # Sort simulations id's to ensure that changing the order of creation doesn't change the id.
        for sim_id in sorted(_sim_ids):
            hasher.update(sim_id)

        del _sim_ids
        self.simulations_id = hasher.hexdigest()

    def init_from_yaml(self, yaml):
        if 'name' not in yaml:
            yaml['name'] = filename_without_ext(yaml['file_name'])

        if 'forecast_date' not in yaml:
            # If there's no forecast date defined, we explicitly define it as None.
            yaml['forecast_date'] = None

        if 'results' not in yaml:
            raise RuntimeError('Missing results property in forecast file %s.' % yaml['file_name'])
            # yaml['results'] = DotDict({'cycle': ['HWAM'], 'daily': []})

        if 'daily' not in yaml['results']:
            yaml['results']['daily'] = []

        if 'cycle' not in yaml['results']:
            yaml['results']['cycle'] = []

        if len(yaml['results']['cycle']) == 0 and len(yaml['results']['daily']) == 0:
            raise RuntimeError('No expected results variables were provided in forecast file %s.' % yaml['file_name'])

        # Keys that belong to simulation objects and must be deleted from a Forecast object.
        simulation_keys = ['initial_conditions', 'agronomic_management', 'site_characteristics']
        for key in simulation_keys:
            if key in yaml:
                del yaml[key]

        self.__dict__.update(DotDict(yaml))

    @property
    def id(self):
        if self.configuration.weather_series in ['historic', 'netcdf']:
            self.simulations_id = 'ref_%s' % self.simulations_id
        return xxh64('%s,%s' % (self.forecast_date, self.simulations_id)).hexdigest()

    @property
    def campaign_start_date(self):
        """
        Calculates the start date of this forecast campaign.
        :rtype : datetime
        """
        f_date = self.forecast_date
        if f_date is None:
            return None
        f_date = datetime.strptime(f_date, '%Y-%m-%d')

        # Se resta 3 al menor mes porque solo se agregan 90 días de datos climaticos antes del
        # primer día del mes de inicio de campanha y 90 días despues del último día del mes de
        # fin de campanha, entonces, restando 3 meses, más o menos se cubre el periodo sin cultivo
        # ya que al restar 3 meses, se retrocede entre 60 y 90 días (dependiendo del día de siembra)
        if f_date.month < min(self.planting_dates()).month - 3:  # self.campaign_first_month:
            return f_date.replace(day=1, month=self.campaign_first_month, year=f_date.year - 1)
        return f_date.replace(day=1, month=self.campaign_first_month)

    @property
    def campaign_end_date(self):
        """
        Calculates the end date of this forecast campaign.
        :rtype : datetime
        """
        f_date = self.forecast_date
        if f_date is None:
            return None
        f_date = datetime.strptime(self.forecast_date, '%Y-%m-%d')

        # Se resta 3 al menor mes porque solo se agregan 90 días de datos climaticos antes del
        # primer día del mes de inicio de campanha y 90 días despues del último día del mes de
        # fin de campanha, entonces, restando 3 meses, más o menos se cubre el periodo sin cultivo
        # ya que al restar 3 meses, se retrocede entre 60 y 90 días (dependiendo del día de siembra)
        if f_date.month < min(self.planting_dates()).month - 3:  # self.campaign_first_month:
            return f_date.replace(day=1, month=self.campaign_first_month) - timedelta(days=1)
        return f_date.replace(day=1, month=self.campaign_first_month, year=f_date.year + 1) - timedelta(days=1)

    @property
    def campaign_name(self):
        start_year = self.campaign_start_date
        if not start_year:
            return "None"
        start_year = start_year.year
        return "%d/%d" % (start_year, start_year+1)

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

    def persistent_view(self):
        """
        Subsets the object to keep only fields that are relevant for persistency.
        :return:
        """
        # Forecasts without a forecast date are just a collection of simulations and shouldn't have a persistent view.
        if self.forecast_date is None or self.configuration.weather_series == 'historic':
            return None

        view = copy.deepcopy(self.__dict__)
        del view['simulations_id']
        del view['paths']
        del view['configuration']
        del view['folder_name']
        del view['weather_stations']
        view['result_variables'] = view['results']
        del view['results']

        if 'job_id' in view:
            del view['job_id']

        view['_id'] = self.id
        view['campaign_name'] = self.campaign_name
        view['locations'] = []
        view['simulations'] = []
        view['forecast_date'] = self.forecast_date
        view['rainfall'] = {
            'start_date': self.campaign_start_date - timedelta(days=90),  # mantener esto sincronizado con CombinedSeriesMaker
            'end_date': self.campaign_end_date + timedelta(days=90),  # mantener esto sincronizado con CombinedSeriesMaker
            'ref_date': WeatherNetCDFWriter.reference_date,
            'data': self.rainfall
        }
        view['run_date'] = datetime.now().strftime("%Y-%m-%d %H-%M-%S.%f")[:-3]

        if 'simulation_count' in view:
            del view['simulation_count']

        for loc_key, loc in self.locations.items():
            view['locations'].append(loc.id)

        return view

    def public_view(self, forecasts_paths):
        view = DotDict(copy.deepcopy(self.__dict__))
        del view['results']
        del view['simulations']

        view['campaign_name'] = self.campaign_name
        view['forecast_date'] = self.forecast_date
        view['forecast_id'] = self.id

        if 'rainfall' in view:
            del view['rainfall']

        view['locations'] = []
        for loc_key, loc in self.locations.items():
            view['locations'].append(loc['name'])

        view['file_name'] = self.public_file_name(forecasts_paths)

        return view.to_json()

    def public_file_name(self, forecasts_paths):
        return self.file_name.replace(forecasts_paths, '')[1:]

    def planting_dates(self):
        dates = []
        for location, simulations in self.simulations.items():
            for simulation in simulations:
                d = simulation.get('management', {}).get('date_1', None)
                if d is None:
                    continue
                d = datetime.strptime('2000'+d[4:], '%Y%m%d')
                if d not in dates:
                    dates.append(d)
        return dates
