from core.lib.dssat.DSSATWthWriter import DSSATWthWriter

__author__ = 'Federico Schmidt'

import logging
from core.modules.simulations_manager.weather_makers.DatabaseWeatherSeries import DatabaseWeatherSeries
import os.path
import time


class CombinedSeriesMaker(DatabaseWeatherSeries):
    def __init__(self, system_config, max_parallelism, weather_writer=None):
        if not weather_writer:
            weather_writer = DSSATWthWriter
        super(CombinedSeriesMaker, self).__init__(system_config, max_parallelism, weather_writer)

    def create_from_db(self, omm_id, forecast):
        wth_output = os.path.join(forecast.paths.wth_csv_export, str(omm_id))

        forecast_date = forecast.forecast_date
        start_date = forecast.campaign_start_date.strftime('%Y-%m-%d')
        end_date = forecast.campaign_end_date.strftime('%Y-%m-%d')

        wth_db_connection = self.system_config.database['weather_db']
        cursor = wth_db_connection.cursor()

        start_time = time.time()
        cursor.execute("SELECT pr_create_campaigns(%s, %s, %s, %s, %s)",
                       (omm_id, start_date, forecast_date, end_date,
                        wth_output))
        logging.getLogger().debug("Station: %s. Date: %s. Time: %s." %
                                  (omm_id, forecast_date, (time.time() - start_time)))
