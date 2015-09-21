import logging
import yaml
from core.lib.jobs.base import BaseJob
from core.lib.utils.extended_collections import DotDict
from core.model.ForecastBuilder import ForecastBuilder
from core.modules.config import priority
from lib.utils.log import log_format_exception
from model.Location import Location

__author__ = 'Federico Schmidt'


class ForecastLoader:

    def __init__(self, jobs_lock, system_config):
        self.jobs_lock = jobs_lock
        self.system_config = system_config

    def load_file(self, forecast_file):
        with self.jobs_lock.parallel_job():
            forecasts = []
            try:
                forecast = DotDict(yaml.safe_load(open(forecast_file)))
                forecast['file_name'] = forecast_file

                for loc_key in forecast['locations'].keys():
                    forecast['locations'][loc_key] = Location(forecast['locations'][loc_key],
                                                              self.system_config.database['weather_db'])

                builder = ForecastBuilder(forecast, self.system_config)
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

    def reload_file(self, forecast_file):
        with self.jobs_lock.blocking_job(priority=priority.LOAD_FORECAST):
            pass
