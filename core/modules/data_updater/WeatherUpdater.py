import logging
import numpy as np
from core.lib.utils.log import log_format_exception
from core.lib.jobs.monitor import NullMonitor, ProgressMonitor
import requests
from core.modules.config.priority import UPDATE_DB_DATA, UPDATE_MAX_WEATHER_DATES, UPDATE_RAINFALL_QUANTILES
from core.lib.jobs.monitor import JOB_STATUS_WAITING, JOB_STATUS_RUNNING
from core.lib.utils.database import DatabaseUtils
from datetime import datetime, timedelta
from core.lib.utils.extended_collections import group_by, DotDict
import io
from core.modules.data_updater.impute import RunImputation

__author__ = 'Federico Schmidt'


class WeatherUpdater:
    def __init__(self, system_config):
        self.system_config = system_config
        self.wth_db = None
        self.weather_stations_ids = set()
        self.wth_max_date = DotDict()

    def add_weather_station_id(self, omm_id):
        try:
            omm_id = int(omm_id)
        except ValueError:
            return False

        self.weather_stations_ids.add(omm_id)

    def update_weather_db(self, progress_monitor=None):
        logging.info('Running weather db update.')

        if not progress_monitor:
            progress_monitor = NullMonitor

        if len(self.weather_stations_ids) == 0:
            return

        # Notify observers we're going to wait for a lock acquisition.
        progress_monitor.job_started(initial_status=JOB_STATUS_WAITING)
        with self.system_config.jobs_lock.blocking_job(priority=UPDATE_DB_DATA):
            # Lock acquired, notify observers.
            progress_monitor.update_progress(job_status=JOB_STATUS_RUNNING)

            # Check weather api configuration.
            if 'weather_update_api' not in self.system_config:
                raise RuntimeError('Weather update API not configured.')

            api_config = self.system_config['weather_update_api']

            if not api_config['url']:
                raise RuntimeError('Missing URL in  weather update API configuration.')

            if 'user' not in api_config:
                raise RuntimeError('Missing username in weather update API configuration.')

            if 'password' not in api_config:
                api_config['password'] = DatabaseUtils.__password_lookup__(api_config['user'],
                                                                           config_path=self.system_config.config_path)

            if not api_config['password']:
                raise RuntimeError('Missing password in weather update API configuration and it couldn\'t be found '
                                   'in the password lookup folder.')

            req_params = {
                'url': api_config['url'],
                'user': api_config['user'],
                'password': api_config['password'],
                'table': 'estacion_registro_diario'
            }

            stations_max_data_date = self.find_max_data_dates(self.weather_stations_ids)
            progress_monitor.end_value = len(stations_max_data_date)

            request_groups = group_by(stations_max_data_date, lambda x: x[1])

            stations_updated = set()
            n_stations_updated = 0
            cursor = None

            try:
                wth_db = self.system_config.database['weather_db']
                cursor = wth_db.cursor()
                cursor.execute('BEGIN TRANSACTION')

                for min_date, omm_ids in request_groups.items():
                    stations_ids = {omm_id[0] for omm_id in omm_ids}

                    n_stations_updated += len(stations_ids)

                    str_ids = {str(omm_id) for omm_id in stations_ids}
                    req_params['omm_ids'] = ','.join(str_ids)
                    req_params['min_date'] = min_date + timedelta(days=1)

                    response = requests.get('%(url)s?login=%(user)s&password=%(password)s'
                                            '&tabla=%(table)s&fecha_desde=%(min_date)s&omm_id=%(omm_ids)s' % req_params)

                    if not response.ok:
                        raise RuntimeError('API request failed (status: %s). Reason: %s.' %
                                           (response.status_code, response.reason))

                    if 'text/csv' not in response.headers['content-type']:
                        raise RuntimeError('Wrong response type in update API: %s.' % response.headers['content-type'])

                    update_data = response.content.decode('utf8').strip().split('\n')

                    progress_monitor.update_progress(new_value=n_stations_updated)

                    # Check if the imputation should be forced.
                    if self.system_config.system_config_yaml.get('force_imputation', False):
                        stations_updated |= self.weather_stations_ids

                    if len(update_data) < 2:
                        continue

                    header = update_data[0].split('\t')
                    update_data = io.StringIO('\n'.join(update_data[1:]))

                    # Insert new data into the database.
                    cursor.copy_from(update_data, 'estacion_registro_diario')
                    stations_updated |= stations_ids  # Extend set.

                if len(stations_updated) > 0:
                    cursor.execute("COMMIT")
                    impute_job = RunImputation(system_config=self.system_config, parent_task_monitor=progress_monitor)

                    stations_to_impute = stations_updated.intersection(self.weather_stations_ids)

                    if len(stations_to_impute) > 0:
                        ret_val = impute_job.start(weather_stations=stations_to_impute)
                    else:
                        ret_val = 0

                    pm = ProgressMonitor()
                    progress_monitor.add_subjob(pm, 'Refresh materialized view')
                    # Refresh materialized view.
                    self.refresh_view(pm)

                    if ret_val == 0:
                        # Update max dates again.
                        self.update_max_dates(run_blocking=False)
                        logging.info('Updated weather data for station(s): %s.' % stations_updated)
                        return 0
                    else:
                        logging.error('Imputation ended with a non zero exit status (%s).' % ret_val)
                        return 2
                else:
                    # No new data, end transaction to avoid holding a lock on tables.
                    cursor.execute("ROLLBACK")
                    logging.info('No new weather data found.')

            except Exception as ex:
                logging.error('Failed to update weather data. Reason: %s' % log_format_exception(ex))

                if cursor:
                    # If there is an open cursor, rollback the transaction.
                    cursor.execute("ROLLBACK")
                    return 2
            finally:
                if cursor:
                    cursor.close()
        return 1

    def update_max_dates(self, progress_monitor=None, run_blocking=True):
        logging.getLogger().info('Running weather series max date update.')

        if len(self.weather_stations_ids) == 0:
            return

        if not progress_monitor:
            progress_monitor = NullMonitor()

        if run_blocking:
            progress_monitor.update_progress(job_status=JOB_STATUS_WAITING)
            # Acquire a blocking job lock with the update weather dates priority.
            with self.system_config.jobs_lock.blocking_job(priority=UPDATE_MAX_WEATHER_DATES):
                # Lock acquired, notify observers.
                progress_monitor.update_progress(job_status=JOB_STATUS_RUNNING)
                self.__update_max_dates__(progress_monitor)
        else:
            self.__update_max_dates__(progress_monitor)

    def __update_max_dates__(self, progress_monitor=None):
        try:
            self.wth_db = self.system_config.database['weather_db']
            cursor = self.wth_db.cursor()

            ids = list(self.weather_stations_ids)

            # Find the max "useful" date for each station.
            # This means the minimum date between: a) the max date that we have data for a given station; and,
            # b) the minimum date that has null values for that same station (a.k.a. the max date that was imputed).

            max_date_query = """
                WITH min_null_date  AS (
                    SELECT omm_id, MIN(fecha) as min_date FROM estacion_registro_diario_completo
                    WHERE (tmax ISNULL OR tmin ISNULL OR prcp ISNULL OR rad ISNULL)
                    GROUP BY omm_id
                )
                SELECT erdc.omm_id, LEAST(MAX(fecha), min_date)
                FROM estacion_registro_diario_completo erdc
                LEFT JOIN min_null_date mnd ON erdc.omm_id = mnd.omm_id
                WHERE erdc.omm_id = ANY(%s) OR erdc.omm_id IN (
                    SELECT DISTINCT e.omm_vecino_id FROM estacion_vecino e
                    WHERE e.omm_id = ANY(%s) AND e.distancia < 200
                ) GROUP BY erdc.omm_id, min_date
            """

            cursor.execute(max_date_query, (ids, ids))

            for record in cursor:
                self.wth_max_date[record[0]] = record[1]

            cursor.close()
            logging.getLogger().info('Updated weather series max date.')
        except Exception as ex:
            logging.getLogger().error('Failed to update weather series max date. Reason: %s.',
                                      log_format_exception())

    def find_max_data_dates(self, omm_ids):
        if len(omm_ids) == 0:
            return {}

        max_dates = {}

        try:
            self.wth_db = self.system_config.database['weather_db']
            cursor = self.wth_db.cursor()

            ids = omm_ids
            if not isinstance(ids, list):
                ids = list(ids)

            max_date_query = """
                SELECT erdc.omm_id, MAX(fecha)
                FROM estacion_registro_diario_completo erdc
                WHERE erdc.omm_id = ANY(%s) OR erdc.omm_id IN (
                    SELECT DISTINCT e.omm_vecino_id FROM estacion_vecino e
                    WHERE e.omm_id = ANY(%s) AND e.distancia < 200
                ) GROUP BY erdc.omm_id
            """

            # Find max dates for stations in use and their neighbors.
            cursor.execute(max_date_query, (ids, ids))

            for record in cursor:
                max_dates[record[0]] = record[1]

            cursor.close()
        except Exception as ex:
            logging.getLogger().error('Failed to find weather series max date. Reason: %s.',
                                      log_format_exception())

        return max_dates

    def refresh_view(self, progress_monitor=None):
        if not progress_monitor:
            progress_monitor = NullMonitor()

        # DROP INDEX IF EXISTS erdi_index;
        # REFRESH MATERIALIZED VIEW estacion_registro_diario_completo;
        # CREATE INDEX erdi_index ON estacion_registro_diario_completo (omm_id, fecha);
        progress_monitor.end_value = 3
        progress_monitor.job_started()
        cursor = self.system_config.database['weather_db'].cursor()
        cursor.execute('DROP INDEX IF EXISTS erdi_index;')
        progress_monitor.update_progress(1)
        cursor.execute('REFRESH MATERIALIZED VIEW estacion_registro_diario_completo;')
        progress_monitor.update_progress(2)
        cursor.execute('CREATE INDEX erdi_index ON estacion_registro_diario_completo (omm_id, fecha);')
        cursor.execute('COMMIT')
        progress_monitor.job_ended()

    def update_rainfall_quantiles(self, omm_ids=None, progress_monitor=None):
        logging.getLogger().info('Running rainfall quantiles update.')

        if not omm_ids:
            omm_ids = self.weather_stations_ids

        if isinstance(omm_ids, int):
            omm_ids = [omm_ids]

        try:
            if not progress_monitor:
                progress_monitor = NullMonitor()

            progress_monitor.start_value = 0
            progress_monitor.end_value = len(omm_ids)

            progress_monitor.update_progress(job_status=JOB_STATUS_WAITING)
            # Acquire a blocking job lock with the update weather dates priority.
            with self.system_config.jobs_lock.blocking_job(priority=UPDATE_RAINFALL_QUANTILES):
                # Lock acquired, notify observers.
                progress_monitor.update_progress(job_status=JOB_STATUS_RUNNING)

                for index, omm_id in enumerate(omm_ids):
                    wth_db = self.system_config.database['weather_db']
                    cursor = wth_db.cursor()
                    cursor.execute('SELECT campaign, sum FROM pr_campaigns_acum_rainfall(%s,%s)', (omm_id,self.system_config.campaign_first_month))

                    np_prcp_sums = WeatherUpdater.parse_rainfalls(cursor)

                    # daily_cursor = wth_db.cursor()
                    # daily_cursor.execute('SELECT campaign, sum FROM pr_campaigns_rainfall(%s)', (omm_id,))
                    #
                    # np_prcp_values = WeatherUpdater.parse_rainfalls(daily_cursor)

                    # Update (or insert) weather quantiles.
                    db = self.system_config.database['yield_db']
                    db.reference_rainfall.update_one(
                        {"omm_id": omm_id},
                        {"$set": {
                            "quantiles": WeatherUpdater.get_quantiles(np_prcp_sums, quantiles=[5, 25, 50, 75, 95])
                        }}, upsert=True)
                    # Update progress information.
                    progress_monitor.update_progress(index)

            logging.getLogger().info('Updated rainfall quantiles for stations %s.' % list(omm_ids))

        except InvalidRainfallValue as e:
            logging.getLogger().error('Failed to update rainfall quantiles. Reason: %s.', e.message)

        except Exception:
            logging.getLogger().error('Failed to update rainfall quantiles. Reason: %s.', log_format_exception())

    @staticmethod
    def parse_rainfalls(cursor):
        prcp_values = []
        last_id = None

        for record in cursor:
            if record[0] != last_id:
                prcp_values.append([])
                last_id = record[0]
            # Check if record[1] (the current rainfall value) isn't None.
            # If it is, the imputation process has failed at some point.
            if record[1] is None:
                raise InvalidRainfallValue()
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


class InvalidRainfallValue(Exception):
    """Exception raised when an invalid rainfall value was detected.

    Attributes:
        message -- explanation of the error
    """
    default_message = "There are missing rainfall values in the Weather DB."

    def __init__(self, message = default_message):
        self.message = message
