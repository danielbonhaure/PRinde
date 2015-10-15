# coding=utf-8
from datetime import datetime
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
        start_date = forecast.campaign_start_date.strftime('%Y-%m-%d')
        end_date = forecast.campaign_end_date.strftime('%Y-%m-%d')
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


if __name__ == '__main__':
    db_connection = DatabaseUtils.connect_postgresql({
        'host': '192.168.1.115',
        'db_name': 'crc_ssa',
        'user': 'crcssa_user',
        'password': 'asdf1234'
    })
    system_config = DotDict({
        'database': {
            'weather_db': db_connection
        }
    })

    f = DotDict({
        'forecast_date': '2014-12-15',
        'campaign_start_date': datetime.strptime('2014-05-01', '%Y-%m-%d'),
        'campaign_end_date': datetime.strptime('2015-04-30', '%Y-%m-%d')
    })
    maker = CombinedSeriesMaker(system_config, 1)
    start = datetime.now()
    series = maker.create_from_db({'omm_id': 87550}, f)
    for s in series:
        pass
    end = datetime.now() # 12:42
    print('Time: %s' % (end-start,))
