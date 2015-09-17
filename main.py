#!/usr/bin/python
# -*- coding: utf-8 -*-
import logging
from apscheduler.executors.pool import ThreadPoolExecutor
import sys
from core.modules.data_updater.WeatherUpdater import WeatherUpdater
from lib.utils.log import log_format_exception
from modules.config.system_config import SystemConfiguration
from core.modules.forecasts_manager.events import register_signals
from core.modules.forecasts_manager.boot import boot_system
from core.modules.forecasts_manager.ForecastManager import ForecastManager
from core.lib.io.file import absdirname
from frontend.web import WebServer
import threading
from lib.jobs.scheduler import MonitoringScheduler
from lib.logging.stream import WebStream
from modules.statistics.StatsCenter import StatsCenter
from datetime import datetime

__author__ = 'Federico Schmidt'


class Main:

    def __init__(self):
        self.system_threads = []
        self.scheduler = None
        self.web_server = None
        self.stats = None
        # Get system root path.
        root_path = absdirname(__file__)

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

    def bootstrap(self):
        boot_system(self.system_config)

        # Register a handler SIGINT/SIGTERM signals.
        register_signals()

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
        web_server_thread.start()
        self.system_threads.append(web_server_thread)
        self.system_config.logger.info("Web server started.")

        # Start the scheduler.
        self.scheduler.start()

        # Instantiate the forecast manager.
        self.forecast_manager = ForecastManager(self.scheduler, self.system_config, self.weather_updater)

        for omm_id in self.system_config.weather_stations_ids:
            self.weather_updater.add_weather_station_id(omm_id)

        # self.scheduler.add_job(self.weather_updater.update_weather_db, name='Update weather database',
        #                        trigger='interval', days=1, next_run_time=datetime.now())
        # Schedule the update of weather data max dates to be ran once a day.
        self.scheduler.add_job(self.weather_updater.update_max_dates, name='Update weather data max dates',
                               trigger='interval', days=1, next_run_time=datetime.now())
        # Schedule the update of rainfall quantiles every 120 days.
        self.scheduler.add_job(self.weather_updater.update_rainfall_quantiles, trigger='interval', days=120,
                               name='Update rainfall quantiles')

        self.forecast_manager.start()
        self.system_config.logger.info("Forecast manager started.")
        self.system_config.logger.info("Done initializing services.")

        # Join threads with main thread. Main thread blocks here.
        for th in self.system_threads:
            th.join()

# Start running the system.
main = Main()
main.run()
