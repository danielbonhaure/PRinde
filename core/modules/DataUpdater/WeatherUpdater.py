import logging
import numpy as np

from core.lib.utils.log import log_format_exception
from lib.jobs.monitor import NullMonitor

__author__ = 'Federico Schmidt'


class WeatherUpdater:
    def __init__(self, system_config):
        self.system_config = system_config
        self.wth_db = None
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
            self.wth_db = self.system_config.database['weather_db']
            cursor = self.wth_db.cursor()
            cursor.execute('SELECT erd.omm_id, MAX(erd.fecha) FROM estacion_registro_diario erd '
                           'WHERE erd.omm_id = ANY(%s) GROUP BY erd.omm_id',
                           (list(self.weather_stations_ids),))

            for record in cursor:
                self.wth_max_date[record[0]] = record[1]
            logging.getLogger().info('Updated weather series max date.')
        except Exception, ex:
            logging.getLogger().error('Failed to update weather series max date. Reason: %s.',
                                      log_format_exception())

    def update_rainfall_quantiles(self, omm_ids=None, progress_monitor=None):
        if not omm_ids:
            omm_ids = self.weather_stations_ids

        if isinstance(omm_ids, int):
            omm_ids = [omm_ids]

        try:
            if not progress_monitor:
                progress_monitor = NullMonitor()

            progress_monitor.start_value = 0
            progress_monitor.end_value = len(omm_ids)

            for index, omm_id in enumerate(omm_ids):
                wth_db = self.system_config.database['weather_db']
                cursor = wth_db.cursor()
                cursor.execute('SELECT campaign, sum FROM pr_campaigns_acum_rainfall(%s)', (omm_id,))

                np_prcp_sums = WeatherUpdater.parse_rainfalls(cursor)

                # daily_cursor = wth_db.cursor()
                # daily_cursor.execute('SELECT campaign, sum FROM pr_campaigns_rainfall(%s)', (omm_id,))
                #
                # np_prcp_values = WeatherUpdater.parse_rainfalls(daily_cursor)

                # Update (or insert) weather quantiles.
                db = self.system_config.database['rinde_db']
                db.reference_rainfall.update_one(
                    {"omm_id": omm_id},
                    {"$set": {
                        "quantiles": WeatherUpdater.get_quantiles(np_prcp_sums, quantiles=[5, 25, 50, 75, 95])
                    }}, upsert=True)
                # Update progress information.
                progress_monitor.update_progress(index)
            logging.getLogger().info('Updated rainfall quantiles for stations %s.' % list(omm_ids))
        except Exception:
            logging.getLogger().error('Failed to update rainfall quantiles. Reason: %s.',
                                      log_format_exception())

    @staticmethod
    def parse_rainfalls(cursor):
        prcp_values = []
        last_id = None

        for record in cursor:
            if record[0] != last_id:
                prcp_values.append([])
                last_id = record[0]
            # Append values to the last campaign (the one we're processing).
            prcp_values[-1].append(float(record[1]))

            # Convert to numpy array.
        np_prcp_values = np.empty(shape=(len(prcp_values), 365))
        for idx, prcps in enumerate(prcp_values):
            np_prcp_values[idx] = prcps[0:365]  # Exclude leap years.

        return np_prcp_values

    @staticmethod
    def get_quantiles(np_arr, quantiles=None, axis=0):
        if not quantiles:
            return {}
        q = {}
        for quantile in quantiles:
            q[str(quantile)] = np.percentile(np_arr, q=quantile, axis=axis).tolist()
        return q
