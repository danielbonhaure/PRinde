# -*- coding: utf-8 -*-
import logging
import os
import threading
import traceback
from core.lib.io.file import create_folder_with_permissions
from core.lib.utils.DotDict import DotDict
from core.lib.utils.log import log_format_exception
from core.modules.PreparadorDeSimulaciones.CombinedSeriesMaker import CombinedSeriesMaker

__author__ = 'Federico Schmidt'

from core.modules.PreparadorDeSimulaciones.WeatherSeriesMaker import WeatherSeriesMaker
from datetime import datetime
from datetime import timedelta
import time


class ForecastManager:

    def __init__(self, system_config):
        self.system_config = system_config

    def start(self):
        # Create scheduler, etc.
        for forecast in self.system_config.forecasts:
            try:
                self.run_forecast(forecast)
            except:
                logging.getLogger("main").error("Failed to run forecast '%s'. Reason: %s" %
                                                (forecast.name, log_format_exception()))

    def run_campaigns(self):
        pass
        # for wth_station in self.system_config.omm_ids:
        #     print("Creating experiment data for station with OMM ID = %s." % wth_station)
        #     # Get soil data.
        #     # Crear series climáticas
        #     self.seriesMaker.create_series(wth_station)
        #     # Escribir archivos de campaña.
        #     # Llamar pSIMS.

    def run_forecast(self, forecast):
        logging.getLogger('main').info('\nRunning forecast "%s".' % forecast.name)
        run_start_time = time.time()
        wth_series_maker = CombinedSeriesMaker(self.system_config, forecast.configuration.max_paralellism)

        if forecast.configuration.weather_series != 'combined':
            raise RuntimeError('Weather series type "%s" unsupported.' % forecast.configuration.weather_series)

        folder_name = "%s - %s" % (datetime.now().isoformat(), forecast['name'])
        forecast.folder_name = folder_name

        # Add folder name to rundir and create it.
        forecast.paths.rundir = os.path.join(forecast.paths.rundir, folder_name)
        create_folder_with_permissions(forecast.paths.rundir)

        # Create a folder for the weather grid inside that rundir.
        forecast.paths.weather_grid_path = os.path.join(forecast.paths.rundir, 'wth')
        create_folder_with_permissions(forecast.paths.weather_grid_path)

        # Create the folder where we'll read the CSV files created by the database.
        forecast.paths.wth_csv_read = os.path.join(forecast.paths.wth_csv_read, folder_name)
        forecast.paths.wth_csv_export = os.path.join(forecast.paths.wth_csv_export, folder_name)
        create_folder_with_permissions(forecast.paths.wth_csv_read)

        forecast['simulations'] = DotDict()

        active_threads = []

        for omm_id in forecast['locations']:
            forecast.simulations[omm_id] = DotDict()
            t = threading.Thread(target=wth_series_maker.create_series, args=(omm_id, forecast))
            active_threads.append(t)
            t.start()

        # Wait for the weather grid to be populated.
        for t in active_threads:
            t.join()

        logging.getLogger('main').info('Finished running forecast "%s" (time=%s).\n' %
                                       (forecast.name, repr(time.time() - run_start_time)))