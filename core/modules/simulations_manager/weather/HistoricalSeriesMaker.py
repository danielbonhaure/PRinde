# coding=utf-8
import itertools
import logging
from core.lib.dssat.DSSATWthWriter import DSSATWthWriter
from core.modules.simulations_manager.weather.DatabaseWeatherSeries import DatabaseWeatherSeries

__author__ = 'Federico Schmidt'


class HistoricalSeriesMaker(DatabaseWeatherSeries):
    def __init__(self, system_config, max_parallelism, weather_writer=None):
        if not weather_writer:
            weather_writer = DSSATWthWriter
        super(HistoricalSeriesMaker, self).__init__(system_config, max_parallelism, weather_writer)

    def create_series(self, location, forecast, extract_rainfall=True):
        # Force the forecast reference date to be 1950: this weather series creator exports all series with dates
        # starting in 1950. See core.lib.SQL.Base Functions.sql > pr_serie_agraria(omm_id int, year_agrario int)
        logging.info('Forecast reference year: %s' % forecast.configuration.reference_year)
        forecast.configuration.reference_year = 1950
        logging.info('Create series for location: ' % location)
        return DatabaseWeatherSeries.create_series(self, location, forecast)

    def create_from_db(self, location, forecast):
        omm_id = location['weather_station']

        wth_db_connection = self.system_config.database['weather_db']
        cursor = wth_db_connection.cursor()

        cursor.execute('SELECT pr_campañas_completas(%s)', (omm_id,))
        full_campaigns = cursor.fetchall()

        for campaign in full_campaigns:
            campaign_year = campaign[0]
            cursor.execute("SELECT * FROM pr_serie_agraria(%s, %s)", (omm_id, campaign_year))
            colnames = [tuple([desc[0] for desc in cursor.description])]
            yield (campaign_year, itertools.chain(colnames, cursor))
