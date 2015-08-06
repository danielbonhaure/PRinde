import logging
import os
import time
from core.lib.dssat.DSSATWthWriter import DSSATWthWriter

__author__ = 'Federico Schmidt'

from core.modules.PreparadorDeSimulaciones.DatabaseWeatherSeries import DatabaseWeatherSeries


class HistoricalSeriesMaker(DatabaseWeatherSeries):

    def __init__(self, system_config, max_paralellism, weather_writer=None):
        if not weather_writer:
            weather_writer = DSSATWthWriter
        super(HistoricalSeriesMaker, self).__init__(system_config, max_paralellism, weather_writer)

    def create_series(self, omm_id, forecast, extract_rainfall=True):
        return DatabaseWeatherSeries.create_series(self, omm_id, forecast, False)

    def create_from_db(self, omm_id, forecast):
        wth_output = os.path.join(forecast.paths.wth_csv_export, str(omm_id))

        forecast_date = forecast.forecast_date

        wth_db_connection = self.system_config.database['weather_db']
        cursor = wth_db_connection.cursor()

        start_time = time.time()
        cursor.execute("SELECT pr_historic_series(%s, %s)", (omm_id, wth_output))
        logging.getLogger("main").debug("Export historic series for station: %s. Forecast Date: %s. Time: %s." %
                                        (omm_id, forecast_date, (time.time() - start_time)))
        pass
