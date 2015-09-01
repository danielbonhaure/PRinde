import logging
import yaml
from core.lib.jobs.base import BaseJob
from core.lib.utils.extended_collections import DotDict
from core.model.ForecastBuilder import ForecastBuilder
from lib.utils.log import log_format_exception

__author__ = 'Federico Schmidt'


class ForecastLoader(BaseJob):

    def __init__(self, jobs_lock, system_config, run_blocking=True):
        super(ForecastLoader, self).__init__()
        self.run_blocking = run_blocking
        self.jobs_lock = jobs_lock
        self.system_config = system_config

    def run(self, forecast_file):
        if self.run_blocking:
            with self.jobs_lock.blocking_job(priority=1):
                self.load_file(forecast_file)
        else:
            with self.jobs_lock.parallel_job():
                self.load_file(forecast_file)

    def load_file(self, forecast_file):
        forecasts = []
        try:
            forecast = DotDict(yaml.safe_load(open(forecast_file)))
            forecast['file_name'] = forecast_file

            builder = ForecastBuilder(forecast, self.system_config.simulation_schema_path)
            builder.replace_alias(self.system_config.alias_dict)
            builder.inherit_config(self.system_config.system_config_yaml)
            # Build and append forecasts.
            for f in builder.build():
                forecasts.append(f)
                self.system_config.weather_stations_ids.update(
                    set([loc['weather_station'] for loc in f.locations.values()])
                )
            self.system_config.forecasts[forecast_file] = forecasts
            logging.getLogger().info('Loaded %d forecasts from file "%s".' % (len(forecasts), forecast_file))
        except Exception:
            logging.getLogger().error("Skipping forecast file '%s'. Reason: %s." %
                                            (forecast_file, log_format_exception()))
