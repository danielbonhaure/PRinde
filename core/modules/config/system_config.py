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
from lib.utils.sync import JobsLock
from modules.config.loaders import ForecastLoader

__author__ = 'Federico Schmidt'


class SystemConfiguration(FileSystemEventHandler):

    def __init__(self, root_path):
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
        self.forecasts_files = []
        self.weather_stations_ids = set()
        self.alias_dict = None
        self.max_paralellism = 1
        self.jobs_lock = None
        self.watch_thread = None
        self.observer = None

    def load(self):
        # Load system configuration from the YAML file and update this object's dictionary to add the keys found
        # in the config file, allowing us to access them using the dot notation (eg. "config.temp_folder" instead of
        # "config.get('temp_folder')", though this is also supported.
        system_config_yaml = DotDict(yaml.safe_load(open(self.system_config_path)))

        if 'max_paralellism' in system_config_yaml:
            if (not isinstance(system_config_yaml.max_paralellism, int)) or (system_config_yaml.max_paralellism < 1):
                raise RuntimeError('Invalid max_paralellism value (%s).' % system_config_yaml.max_paralellism)

        # Create the system's job syncing lock.
        self.jobs_lock = JobsLock(max_parallel_tasks=self.max_paralellism)

        self.system_config_yaml = system_config_yaml
        self.__dict__.update(system_config_yaml)

        # Load databases configurations and open connections.
        db_config = yaml.safe_load(open(self.databases_config_path))

        self.__dict__['database'] = dict()

        for db_conn, properties in db_config.items():
            if 'type' not in properties:
                raise RuntimeError('Missing database type for database connection "%s".' % db_conn)

            properties['name'] = db_conn

            if properties['type'] == 'postgresql':
                connection = DatabaseUtils.connect_postgresql(properties, self.config_path)
            elif properties['type'] == 'mongodb':
                connection = DatabaseUtils.connect_mongodb(properties, self.config_path)
            else:
                raise RuntimeError('Unsupported database type: "%s".' % properties['type'])

            self.__dict__['database'][db_conn] = connection

        # Load forecasts.
        self.forecasts = {}

        self.alias_dict = None
        if self.alias_keys_path:
            self.alias_dict = yaml.load(open(self.alias_keys_path, 'r'))

        self.forecasts_files = listdir_fullpath(self.forecasts_path, onlyFiles=True, recursive=True,
                                                filter=(lambda x: x.endswith('yaml')))

        loader = ForecastLoader(jobs_lock=self.jobs_lock, system_config=self, run_blocking=False)
        _threads = []

        for file_name in self.forecasts_files:
            t = threading.Thread(target=loader.start, args=(file_name,))
            _threads.append(t)
            t.start()

        for t in _threads:
            # Wait till all forecasts were loaded.
            t.join()
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
        if not self.watch_thread:
            # Create a Thread with "watch" function and start it.
            self.watch_thread = threading.Thread(target=self.watch)
            #self.watch_thread.start()

        #return self.watch_thread
        return None

    def get(self, key, default=None):
        """
        Provides a Python's dictionary alike way of accessing configuration properties, with default values in case
        of missingness.
        :param key: The key you're looking for in the configuration.
        :param default: The value that should be returned if the key is not found.
        :return: The value associated with the key provided, or the default value if not found.
        """
        return self.__dict__.get(key, default)

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
            self.load()
        pass

    def public_view(self):
        view = DotDict(copy.copy(self.__dict__))
        del view['database']
        del view['forecasts']
        del view['logger']
        del view['watch_thread']
        del view['observer']
        del view['alias_dict']
        del view['jobs_lock']
        del view['system_config_yaml']
        del view['forecasts_files']
        return view.to_json()