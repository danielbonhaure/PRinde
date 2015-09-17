import copy
import yaml
import os.path
import threading
import logging
import logging.config
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from core.lib.io.file import listdir_fullpath
from core.lib.utils.extended_collections import DotDict
from core.lib.utils.database import DatabaseUtils
from lib.jobs.monitor import NullMonitor, JOB_STATUS_WAITING, JOB_STATUS_RUNNING
from lib.sync import JobsLock
from modules.config.database_check import CheckWeatherDB, CheckRindeDB
from modules.config.loaders import ForecastLoader
from modules.config.priority import LOAD_CONFIGURATION

__author__ = 'Federico Schmidt'


class SystemConfiguration(DotDict, FileSystemEventHandler):

    def __init__(self, root_path):
        super(SystemConfiguration, self).__init__()

        self.weather_stations_ids = None
        self.root_path = root_path
        self.config_path = os.path.join(root_path, 'config')

        # Create system configuration path relative to root.
        self.system_config_path = os.path.join(self.config_path, 'system.yaml')
        self.alias_keys_path = os.path.join(self.config_path, 'alias.json')
        self.databases_config_path = os.path.join(self.config_path, 'database.yaml')
        self.forecasts_path = os.path.join(self.config_path, 'forecasts')

        if not os.path.isfile(self.system_config_path) or not os.path.exists(self.system_config_path):
            raise RuntimeError('System configuration file not found.')

        if not os.path.isfile(self.databases_config_path) or not os.path.exists(self.databases_config_path):
            raise RuntimeError('Database configuration file not found ("%s").' % self.databases_config_path)

        if not os.path.isfile(self.alias_keys_path):
            logging.getLogger().warning("Alias keys file not found, forecasts keys must match pSIMS keys.")
            self.alias_keys_path = None

        # Find and validate JSON schema for Simulation objects.
        self.simulation_schema_path = os.path.join(self.config_path, 'schema', 'simulation.json')
        if not os.path.isfile(self.simulation_schema_path) or not os.path.exists(self.simulation_schema_path):
            raise RuntimeError('Simulation schema JSON file not found ("%s").' % self.simulation_schema_path)

        self.log_format = '%(asctime)s - %(name)s - %(module)s:%(lineno)s - %(levelname)s - %(message)s'

        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)

        # werk = logging.getLogger('werkzeug')
        # werk.setLevel(logging.DEBUG)

        logging.getLogger('apscheduler.executors.default').setLevel(logging.WARN)
        logging.getLogger('apscheduler.executors').setLevel(logging.WARN)
        logging.getLogger('apscheduler.scheduler').setLevel(logging.WARN)
        logging.getLogger("requests").setLevel(logging.WARN)

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(logging.Formatter(self.log_format))
        self.logger.addHandler(ch)
        # werk.addHandler(ch)

        fh = logging.FileHandler('run.log', mode='w')
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter(self.log_format))
        self.logger.addHandler(fh)
        # werk.addHandler(fh)

        self.system_config_yaml = {}
        self.forecasts = {}
        self.forecasts_files = None
        self.weather_stations_ids = set()
        self.alias_dict = None
        self.max_parallelism = 1
        self.jobs_lock = None
        self.watch_thread = None
        self.observer = None

    @staticmethod
    def load(config_object):
        # Load system configuration from the YAML file and update this object's dictionary to add the keys found
        # in the config file, allowing us to access them using the dot notation (eg. "config.temp_folder" instead of
        # "config.get('temp_folder')", though this is also supported.
        system_config_yaml = DotDict(yaml.safe_load(open(config_object.system_config_path)))

        if 'max_parallelism' in system_config_yaml:
            max_parallelism = system_config_yaml['max_parallelism']
            if (not isinstance(max_parallelism, int)) or (max_parallelism < 1):
                raise RuntimeError('Invalid max_parallelism value (%s).' % max_parallelism)

        config_object.system_config_yaml = system_config_yaml  # This will be used by forecasts to inherit the global config.
        # Update this class __dict__ property to add the properties defined in the config yaml.
        config_object.update(system_config_yaml)

        # Create the system's job syncing lock if it doesn't exist.
        if not config_object.jobs_lock:
            config_object.jobs_lock = JobsLock(max_parallel_tasks=config_object.max_parallelism)
        else:
            # Otherwise, just update the max concurrent parallel jobs. If we reinstantiate it we'll leave
            # every thread waiting for a lock permanently blocked.
            config_object.jobs_lock.max_concurrent_readers = config_object.max_parallelism

        # Load databases configurations and open connections.
        db_config = yaml.safe_load(open(config_object.databases_config_path))

        config_object['database_config'] = DotDict()
        config_object['database'] = DotDict()

        for db_conn, properties in db_config.items():
            if 'type' not in properties:
                raise RuntimeError('Missing database type for database connection "%s".' % db_conn)

            properties['name'] = db_conn

            if properties['type'] == 'postgresql':
                connection = DatabaseUtils.connect_postgresql(properties, config_object.config_path)
            elif properties['type'] == 'mongodb':
                connection = DatabaseUtils.connect_mongodb(properties, config_object.config_path)
            else:
                raise RuntimeError('Unsupported database type: "%s".' % properties['type'])

            # Store connection config dictionary and connection instance.
            config_object['database_config'][db_conn] = properties
            config_object.database[db_conn] = connection

        wth_db_checker = CheckWeatherDB(system_config=config_object)
        wth_db_checker.start()

        rinde_db_checker = CheckRindeDB(system_config=config_object)
        rinde_db_checker.start()


        config_object.alias_dict = None
        if config_object.alias_keys_path:
            config_object.alias_dict = yaml.load(open(config_object.alias_keys_path, 'r'))

        # Load forecasts.
        if not config_object.forecasts:
            config_object.forecasts = {}

        forecast_file_list = listdir_fullpath(config_object.forecasts_path, onlyFiles=True, recursive=True,
                                              filter=(lambda x: x.endswith('yaml')))

        if not config_object.forecasts_files:
            config_object.forecasts_files = forecast_file_list
        else:
            # If this property was already initialized, find if there are new files.
            for forecast_file in forecast_file_list:
                # If this file was already added to the property, remove it from the list we'll return to the caller.
                if forecast_file in config_object.forecasts_files:
                    forecast_file_list.remove(forecast_file)

        return forecast_file_list

        #     try:
        #         forecast = DotDict(yaml.safe_load(open(file_name)))
        #         forecast['file_name'] = file_name
        #
        #         builder = ForecastBuilder(forecast, self.simulation_schema_path)
        #         builder.replace_alias(alias_dict)
        #         builder.inherit_config(system_config)
        #         # Build and append forecasts.
        #         for f in builder.build():
        #             self.__dict__['forecasts'].append(f)
        #             station_ids.update(set([loc['weather_station'] for loc in f.locations.values()]))
        #     except Exception:
        #         logging.getLogger().error("Skipping forecast file '%s'. Reason: %s." %
        #                                         (file_name, log_format_exception()))
        #
        # self.__dict__['weather_stations_ids'] = station_ids

        # If the watch thread isn't already loaded, create and start it.
        # if not config_object.watch_thread:
        #     # Create a Thread with "watch" function and start it.
        #     config_object.watch_thread = threading.Thread(target=config_object.watch)
        #     #self.watch_thread.start()

        # return self.watch_thread

    @staticmethod
    def load_forecasts(config_object, forecast_list):
        loader = ForecastLoader(jobs_lock=config_object.jobs_lock, system_config=config_object, run_blocking=False)
        _threads = []

        for file_name in forecast_list:
            t = threading.Thread(target=loader.start, args=(file_name,))
            _threads.append(t)
            t.start()

        for t in _threads:
            # Wait till all forecasts were loaded.
            t.join()

    def reload(self, progress_monitor=None):
        if not progress_monitor:
            progress_monitor = NullMonitor()

        progress_monitor.job_started()
        progress_monitor.update_progress(job_status=JOB_STATUS_WAITING)

        with self.jobs_lock.blocking_job(priority=LOAD_CONFIGURATION):
            progress_monitor.update_progress(job_status=JOB_STATUS_RUNNING)
            # Create a temporal backup of the current configuration.
            config_clone = copy.copy(self)
            try:
                new_forecasts_files = SystemConfiguration.load(config_clone)
                # If load succeeded, update this class with the new config.
                self.update(config_clone)
                SystemConfiguration.load_forecasts(config_object=self, forecast_list=new_forecasts_files)
            except Exception, ex:
                raise RuntimeError('Configuration not updated. An exception was raised: %s' % ex)

    def watch(self):
        """
        This function is a new thread, instantiates the watchdog.Observer thread and registers this object (self)
        as the Event Handler (see watchdog's documentation).
        :return: None
        """
        # We'll watch the system's config parent path.
        watch_path = os.path.dirname(self.config_path)

        self.observer = Observer()
        self.observer.schedule(self, watch_path, recursive=True)
        self.observer.start()
        self.observer.join()

    def on_modified(self, event):
        """
        Overrides the FileSystemEventHandler.on_modified function, will be triggered every time a file or directory
        changes inside the configuration folder.
        :param event: The object representing the event, see Watchdog's documentation.
        :return: None
        """
        if not event.is_directory and \
                event.src_path.startswith(self.config_path) and \
                event.src_path.endswith('yaml'):
            # Reload config.
            pass
            # self.load

    def public_view(self):
        view = DotDict(copy.copy(self.__dict__))
        del view['database']
        del view['database_config']
        del view['forecasts']
        del view['logger']
        del view['watch_thread']
        del view['observer']
        del view['alias_dict']
        del view['jobs_lock']
        del view['system_config_yaml']
        del view['forecasts_files']
        return view.to_json()
