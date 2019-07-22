from pymongo.errors import BulkWriteError
from core.lib.jobs.base import BaseJob
from core.lib.jobs.monitor import ProgressMonitor, JOB_STATUS_WAITING, JOB_STATUS_RUNNING
from paramiko.ssh_exception import NoValidConnectionsError, AuthenticationException, BadAuthenticationType
from invoke.exceptions import UnexpectedExit
from fabric import Connection
import logging

__author__ = 'Federico Schmidt'


class YieldDatabaseSync(BaseJob):

    def __init__(self, system_config):
        super(YieldDatabaseSync, self).__init__(progress_monitor=ProgressMonitor(end_value=4))
        self.system_config = system_config

    def run(self):
        logging.info('Running yield database synchronization.')
        self.progress_monitor.update_progress(job_status=JOB_STATUS_WAITING)

        # Acquire a read lock (parallel job).
        with self.system_config.jobs_lock.parallel_job():
            self.progress_monitor.update_progress(job_status=JOB_STATUS_RUNNING)
            if 'yield_sync_db' in self.system_config.database:
                source_db = self.system_config.database['yield_db']
                target_db = self.system_config.database['yield_sync_db']

                new_forecasts = self.__find_collection_diff__(collection_name='forecasts', source_db=source_db,
                                                              target_db=target_db)

                if new_forecasts.count() > 0:
                    forecasts_insert_monitor = ProgressMonitor(end_value=new_forecasts.count())
                    self.progress_monitor.add_subjob(forecasts_insert_monitor, job_name='Synchronize forecasts')
                    inserted_forecasts_count = 0

                    # Sync new forecasts.
                    for f in new_forecasts:
                        simulations_ids = f['simulations']

                        bulk_op = target_db.simulations.initialize_unordered_bulk_op()

                        # Fetch this forecast' simulations.
                        for simulation in source_db.simulations.find({'_id': {'$in': simulations_ids}}):
                            bulk_op.insert(simulation)

                        try:
                            bulk_op.execute()
                        except BulkWriteError as bwe:
                            # Check if every error that was raised was a duplicate key error (11000).
                            for err in bwe.details['writeErrors']:
                                if err['code'] != 11000:
                                    raise RuntimeError('Non recoverable error found while trying to sync yield '
                                                       'databases. Details: %s' % bwe.details)

                        target_db.forecasts.insert(f)
                        inserted_forecasts_count += 1
                        forecasts_insert_monitor.update_progress(inserted_forecasts_count)

                # Notify we finished syncing forecasts (the first part of the job).
                self.progress_monitor.update_progress(new_value=1)

                # Sync new reference simulations.
                self.__insert_missing_documents__(collection_name='reference_simulations',
                                                  source_db=source_db,
                                                  target_db=target_db)

                self.progress_monitor.update_progress(new_value=2)

                # Sync new locations.
                self.__insert_missing_documents__(collection_name='locations',
                                                  source_db=source_db,
                                                  target_db=target_db)

                self.progress_monitor.update_progress(new_value=3)

                # Sync new reference rainfalls.
                self.__insert_missing_documents__(collection_name='reference_rainfall',
                                                  id_field='omm_id',
                                                  source_db=source_db,
                                                  target_db=target_db)

                self.progress_monitor.update_progress(new_value=4)

                # Sync new soils.
                self.__insert_missing_documents__(collection_name='soils',
                                                  source_db=source_db,
                                                  target_db=target_db)

        logging.info('Yield database synchronization finished.')

        logging.info('Restarting shiny-server in front-end.')

        paramiko_logger = logging.getLogger("paramiko.transport")
        paramiko_logger.setLevel(logging.ERROR)
        invoke_logger = logging.getLogger("invoke")
        invoke_logger.setLevel(logging.ERROR)
        fabric_logger = logging.getLogger("fabric")
        fabric_logger.setLevel(logging.ERROR)

        # ssh-keygen -f "/home/${USER}/.ssh/known_hosts" -R "${frontend_ip}"
        # ssh-copy-id root@${frontend_ip}

        try:
            with Connection(host=target_db.client.address[0], user='root') as conn:
                result = conn.run('service shiny-server restart', hide=True)
                if result.ok:
                    logging.info('Shiny-server restarted successfully')
                else:
                    logging.warning('Shiny-server restart failed: return code {}, error {}'.format(result.exited,
                                                                                                   result.stderr))
        except NoValidConnectionsError as ex:
            logging.warning('Shiny-server restart failed, conection error: {}'.format(ex.strerror))
        except BadAuthenticationType as ex:
            logging.warning('Shiny-server restart failed, bad authentication type: {}'.format(ex))
        except AuthenticationException as ex:
            logging.warning('Shiny-server restart failed, authentication error: {}'.format(ex))
        except UnexpectedExit as ex:
            logging.warning('Shiny-server restart failed, unexpected exit error: {}'.format(ex.result.stderr.rstrip()))
        except Exception as ex:
            logging.warning('Shiny-server restart failed, error: {}'.format(ex))
            logging.warning('Shiny-server restart failed, do it manually!!')

    def __insert_missing_documents__(self, collection_name, source_db, target_db, id_field='_id'):
        """
        Finds documents inside the given collection that are present in the source database but not in the target
        database and inserts them.
        :param collection_name: A collection name.
        :param source_db: Source database Pymongo connection.
        :param target_db: Target database Pymongo connection.
        """
        new_documents = self.__find_collection_diff__(collection_name, source_db, target_db, id_field)

        if new_documents.count() > 0:
            bulk_operator = target_db[collection_name].initialize_unordered_bulk_op()

            for document in new_documents:
                bulk_operator.insert(document)

            bulk_operator.execute()

    def __find_collection_diff__(self, collection_name, source_db, target_db, id_field='_id'):
        """
        Finds documents inside the given collection that are present in the source database but not in the target
        database.
        :param collection_name: A collection name.
        :param source_db: Source database Pymongo connection.
        :param target_db: Target database Pymongo connection.
        :returns The list of documents that are missing in the target_db's collection.
        """
        found_ids = target_db[collection_name].distinct(id_field)
        # Find documents in source DB with id's that are NOT in the "found_ids" list.
        return source_db[collection_name].find({
            id_field: {'$nin': found_ids}
        })
