# coding=utf-8
from datetime import datetime, timedelta
from psycopg2.extras import DictCursor
from core.lib.dssat.DSSATWthWriter import DSSATWthWriter
from core.lib.utils.database import DatabaseUtils
from core.lib.utils.extended_collections import DotDict
from core.modules.simulations_manager.weather.DatabaseWeatherSeries import DatabaseWeatherSeries
import itertools

__author__ = 'Federico Schmidt'


class CombinedSeriesMaker(DatabaseWeatherSeries):
    def __init__(self, system_config, max_parallelism, weather_writer=None):
        if not weather_writer:
            weather_writer = DSSATWthWriter
        super(CombinedSeriesMaker, self).__init__(system_config, max_parallelism, weather_writer)

    def create_from_db(self, location, forecast):
        forecast_date = forecast.forecast_date
        # Export 90 extra days from the database to avoid missing yields on crops that should be harvested a few days
        # after the campaign ends.
        start_date = (forecast.campaign_start_date - timedelta(days=90)).strftime('%Y-%m-%d')
        end_date = (forecast.campaign_end_date + timedelta(days=90)).strftime('%Y-%m-%d')
        omm_id = location['weather_station']

        wth_db_connection = self.system_config.database['weather_db']
        cursor = wth_db_connection.cursor()

        cursor.execute('SELECT pr_campañas_completas(%s)', (omm_id,))
        full_campaigns = cursor.fetchall()

        for campaign in full_campaigns:
            campaign_year = campaign[0]
            cursor.execute("SELECT * FROM pr_crear_serie(%s, %s, %s, %s, %s)",
                           (omm_id, start_date, forecast_date, end_date, campaign_year))
            colnames = [tuple([desc[0] for desc in cursor.description])]
            yield (campaign_year, itertools.chain(colnames, cursor))
