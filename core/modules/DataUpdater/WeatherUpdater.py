import logging
from core.lib.utils.log import log_format_exception

__author__ = 'Federico Schmidt'


class WeatherUpdater:

    def __init__(self, system_config):
        self.config = system_config
        self.wth_db = self.config.database['weather_db']
        self.weather_stations_ids = set()
        self.wth_max_date = {}

    def add_weather_station_id(self, omm_id):
        try:
            omm_id = int(omm_id)
        except ValueError:
            return False

        self.weather_stations_ids.add(omm_id)

    def update_max_dates(self):
        try:
            cursor = self.wth_db.cursor()
            cursor.execute('SELECT erd.omm_id, MAX(erd.fecha) FROM estacion_registro_diario erd '
                           'WHERE erd.omm_id = ANY(%s) GROUP BY erd.omm_id',
                           (list(self.weather_stations_ids),))

            for record in cursor:
                self.wth_max_date[record[0]] = record[1]
            logging.getLogger('main').info('Updated weather series max date.')
        except Exception, ex:
            logging.getLogger('main').error('Failed to update weather series max date. Reason: %s.',
                                            log_format_exception())
