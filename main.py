#!/usr/bin/python
# -*- coding: utf-8 -*-
import signal

__author__ = 'Federico Schmidt'

import os
import yaml

from core.modules.DataUpdater.WeatherUpdater import WeatherUpdater
from apscheduler.schedulers.background import BlockingScheduler
from core.modules.Configuration import Configuration
from core.modules.GestorDelPronostico.events import register_signals
from core.modules.GestorDelPronostico.boot import boot_system
from core.modules.GestorDelPronostico.ForecastManager import ForecastManager
from core.lib.io.file import absdirname
from frontend.init import UI



# Main class definition.
class Main:

    def __init__(self):
        self.system_threads = []
        self.scheduler = None
        self.ui = None
        # Get system root path.
        root_path = absdirname(__file__)

        self.system_config = Configuration(root_path)
        watch_thread = self.system_config.load()

        if watch_thread:
            self.system_threads.append(watch_thread)

        self.forecast_manager = None
        self.weather_updater = WeatherUpdater(self.system_config)

    def bootstrap(self):
        boot_system(self.system_config)

        # Register a handler SIGINT/SIGTERM signals.
        register_signals()

    def run(self):
        scheduler = BlockingScheduler()
        self.bootstrap()

        self.forecast_manager = ForecastManager(scheduler, self.system_config)

        for omm_id in self.system_config.weather_stations_ids:
            self.weather_updater.add_weather_station_id(omm_id)

        self.weather_updater.update_max_dates()

        scheduler.add_job(self.weather_updater.update_max_dates, trigger='interval', days=1,
                          name='Update max weather data dates')

        # fm = self.forecast_manager.__run__()
        #self.system_threads.append(fm)
        self.scheduler = scheduler
        self.ui = UI(self)
        self.ui.start()

        # Process blocking starts here.
        scheduler.start()



# Start running the system.
main = Main()
main.run()



