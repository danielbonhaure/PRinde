import os

from core.lib.dssat.DSSATWthWriter import DSSATWthWriter
from core.modules.simulations_manager.weather.csv.CSVDatabaseWeatherSeries import CSVDatabaseWeatherSeries

__author__ = 'Federico Schmidt'


class CSVHistoricalSeriesMaker(CSVDatabaseWeatherSeries):
    def __init__(self, system_config, max_parallelism, weather_writer=None):
        if not weather_writer:
            weather_writer = DSSATWthWriter
        super(CSVHistoricalSeriesMaker, self).__init__(system_config, max_parallelism, weather_writer)

    def create_series(self, location, forecast, extract_rainfall=True):
        return CSVDatabaseWeatherSeries.create_series(self, location, forecast)

    def create_from_db(self, omm_id, forecast):
        wth_output = os.path.join(forecast.paths.wth_csv_export, str(omm_id))

        forecast_date = forecast.forecast_date

        wth_db_connection = self.system_config.database['weather_db']
        cursor = wth_db_connection.cursor()

        # start_time = time.time()
        cursor.execute("SELECT pr_historic_series(%s, %s)", (omm_id, wth_output))
        # logging.getLogger().debug("Export historic series for station: %s. Forecast Date: %s. Time: %s." %
        #                           (omm_id, forecast_date, (time.time() - start_time)))
