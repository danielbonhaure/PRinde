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
        self.campaign_first_month = system_config.campaign_first_month

    def create_series(self, location, forecast, extract_rainfall=True):
        # Force the forecast reference date to be 1950: this weather series creator exports all series with dates
        # starting in 1950. See core.lib.SQL.Base Functions.sql > pr_serie_agraria(omm_id int, year_agrario int)
        forecast.configuration.reference_year = 1950

        planting_after_new_year = [d.month < forecast.campaign_first_month for d in forecast.planting_dates()]
        if all(planting_after_new_year):
            forecast.configuration.reference_year = 1951

        if any(planting_after_new_year) and any([not i for i in planting_after_new_year]):
            raise RuntimeError('Forecast "%s" contains planting dates that happen both before and after the new year. '
                               'This is currently not supported since pSIMS needs a unique reference year configured. '
                               'Plase consider splitting the conflicting locations in two forecasts.' % forecast.name)

        return DatabaseWeatherSeries.create_series(self, location, forecast)

    def create_from_db(self, location, forecast):
        omm_id = location['weather_station']

        wth_db_connection = self.system_config.database['weather_db']
        cursor = wth_db_connection.cursor()

        cursor.execute('SELECT pr_campañas_completas(%s, %s)', (omm_id, self.campaign_first_month))
        full_campaigns = cursor.fetchall()

        for campaign in full_campaigns:
            campaign_year = campaign[0]
            cursor.execute("SELECT * FROM pr_serie_agraria(%s, %s, %s)",
                           (omm_id, campaign_year, self.campaign_first_month))
            colnames = [tuple([desc[0] for desc in cursor.description])]
            yield (campaign_year, itertools.chain(colnames, cursor))
