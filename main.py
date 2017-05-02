#!/usr/bin/python
# -*- coding: utf-8 -*-
import logging
import os
from core.lib.io.file import absdirname

# Get system root path.
root_path = absdirname(__file__)
# Change the current working directory to the root path.
os.chdir(root_path)

import sys
import signal
import threading
from datetime import datetime
import time
from apscheduler.executors.pool import ThreadPoolExecutor
from core.modules.data_updater.WeatherUpdater import WeatherUpdater
from core.lib.utils.log import log_format_exception
from core.modules.config.system_config import SystemConfiguration
from core.modules.config.boot import boot_system
from core.modules.simulations_manager.ForecastManager import ForecastManager
from core.modules.data_updater.sync import YieldDatabaseSync
from core.lib.jobs.scheduler import MonitoringScheduler
from core.lib.logging.stream import WebStream
from core.modules.statistics.StatsCenter import StatsCenter
from frontend.web import WebServer
from core.modules.data_updater.SoilsUpdater import SoilsUpdater

__author__ = 'Federico Schmidt'


class Main:

    def __init__(self):
        self.system_threads = []
        self.scheduler = None
        self.web_server = None
        self.stats = None

        self.system_config = None

        try:
            self.system_config = SystemConfiguration(root_path)
            forecasts_files = SystemConfiguration.load(self.system_config)
            SystemConfiguration.load_forecasts(self.system_config, forecasts_files)
        except Exception, ex:
            logging.getLogger().error('Failed to load system configuration. Reason: %s.' % log_format_exception(ex))
            sys.exit(1)

        self.forecast_manager = None
        self.weather_updater = WeatherUpdater(self.system_config)
        self.db_sync = YieldDatabaseSync(self.system_config)
        self.soils_updater = SoilsUpdater(self.system_config)

    def bootstrap(self):
        boot_system(self.system_config)

        # Register a handler SIGINT/SIGTERM signals.
        # register_signals()

        def shutdown_handler(*args):
            self.stop(*args)
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

    def run(self):
        # Order of init should be:
        #  1. call self.bootstrap() - this checks wether paths in the config file exist or can be created,
        #                            among other things.
        #  2. instantiate the scheduler.
        #  3. instantiate the StatsCenter, passing the scheduler as argument.
        #  4. instantiate the WebServer, passing the StatsCenter and the scheduler and the config as arguments.
        #  5. configure the web server log handle and start the WebServer.
        #  6. start the scheduler.
        #  7. instantiate the Forecast Manager.
        self.bootstrap()
        self.scheduler = MonitoringScheduler(excecutors={
            'default': ThreadPoolExecutor(max_workers=self.system_config.max_parallelism)
        }, job_defaults={
            'coalesce': True,
            'misfire_grace_time': 23*60*60  # 23 hours of grace time
        })
        self.stats = StatsCenter(self.scheduler)
        self.web_server = WebServer(self.stats, self.scheduler, self.system_config)

        # Register progress listeners.
        self.scheduler.add_progress_listener(self.web_server)
        self.scheduler.add_progress_listener(self.stats)

        # Configure a log handler to stream logs to users through the web server.
        web_log_stream = logging.StreamHandler(stream=WebStream(self.web_server))
        web_log_stream.setFormatter(logging.Formatter(self.system_config.log_format))
        self.system_config.logger.addHandler(web_log_stream)
        self.system_config.logger.info("System startup.")

        # Start the web server so we can start monitoring system tasks.
        web_server_thread = threading.Thread(target=self.web_server.start, name='Webserver')
        web_server_thread.daemon = True
        web_server_thread.start()
        self.system_threads.append(web_server_thread)
        self.system_config.logger.info("Web server started.")

        # Start the scheduler.
        self.scheduler.start()

        # Instantiate the forecast manager.
        self.forecast_manager = ForecastManager(self.scheduler, self.system_config, self.weather_updater)
        self.web_server.forecast_manager = self.forecast_manager

        for omm_id in self.system_config.weather_stations_ids:
            self.weather_updater.add_weather_station_id(omm_id)

        self.scheduler.add_job(self.weather_updater.update_weather_db, name='Update weather database',
                               trigger='interval', days=1, next_run_time=datetime.now())
        # Schedule the update of weather data max dates to be ran once a day.
        self.scheduler.add_job(self.weather_updater.update_max_dates, name='Update weather data max dates',
                               trigger='interval', days=1, next_run_time=datetime.now())
        # Schedule the update of rainfall quantiles every 120 days.
        self.scheduler.add_job(self.weather_updater.update_rainfall_quantiles, trigger='interval', days=120,
                               name='Update rainfall quantiles')
        # Schedule the synchronization of yield databases (between backend and frontend).
        # This is actually performed only if such database is defined inside the config/database.yaml file.
        self.scheduler.add_job(self.db_sync, trigger='interval', days=1, name='Synchronize yield databases')

        # Schedule the update of soils every 120 days.
        self.scheduler.add_job(self.soils_updater, trigger='interval', days=120, name='Update soils metrics')

        self.forecast_manager.start()
        self.system_config.logger.info("Forecast manager started.")
        self.system_config.logger.info("Done initializing services.")

        # Block main thread.
        while True:
            # Sleep 1 day.
            time.sleep(86400)

    def stop(self, *args):
        self.scheduler.shutdown(wait=False)
        raise KeyboardInterrupt

main = Main()
# Start running the system.
main.run()
