# -*- coding: utf-8 -*-
import json
import logging
import os
import shutil
import threading
import random
from core.lib.io.file import create_folder_with_permissions
from core.lib.utils.DotDict import DotDict
from core.modules.PreparadorDeSimulaciones.CombinedSeriesMaker import CombinedSeriesMaker
from core.modules.PreparadorDeSimulaciones.HistoricalSeriesMaker import HistoricalSeriesMaker

__author__ = 'Federico Schmidt'

from core.modules.PreparadorDeSimulaciones.WeatherSeriesMaker import WeatherSeriesMaker
from datetime import datetime
from datetime import timedelta
import time
import copy
import yaml


class ForecastManager:

    def __init__(self, system_config):
        self.system_config = system_config
        self.synth_values = {
            '2014-11-01': {
                'min': 1500.,
                'max': 5300.
            },
            '2014-12-01': {
                'min': 1500.,
                'max': 5000.
            },
            '2015-01-01': {
                'min': 2300.,
                'max': 5000.
            },
            '2015-02-01': {
                'min': 4000.,
                'max': 5300.
            }
        }

    def start(self):
        # Create scheduler, etc.
        for forecast in self.system_config.forecasts:
            # TODO: Envolver nuevamente run_forecast en try-catch.
            # try:
            self.run_forecast(forecast)
            # except:
            #     logging.getLogger("main").error("Failed to run forecast '%s'. Reason: %s" %
            #                                     (forecast.name, log_format_exception()))

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
        forecast = copy.deepcopy(forecast)
        logging.getLogger('main').info('\nRunning forecast "%s" (%s).' % (forecast.name, forecast.forecast_date))
        run_start_time = time.time()

        # Get MongoDB connection.
        db = self.system_config.database['rinde_db']

        wth_series_maker = None

        if forecast.configuration.weather_series == 'combined':
            wth_series_maker = CombinedSeriesMaker(self.system_config, forecast.configuration.max_paralellism)
        elif forecast.configuration.weather_series == 'historic':
            wth_series_maker = HistoricalSeriesMaker(self.system_config, forecast.configuration.max_paralellism)
        else:
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

        active_threads = []

        forecast.weather_stations = {}
        forecast.rainfall = {}

        for loc_key, location in forecast['locations'].iteritems():
            if 'weather_station' in location:
                omm_id = location['weather_station']
            else:
                # TODO: Buscar estación más cercana.
                raise RuntimeError('Weather station lookup not implemented yet.')

            # Query DB to get location ID.
            loc = db.locations.find_one({
                "name": location.name,
                "coord_x": location.coord_x,
                "coord_y": location.coord_y,
                "weather_station": location.weather_station
            })

            # If found, update location ID, otherwise insert it.
            if loc:
                location._id = loc['_id']
            else:
                location._id = db.locations.insert_one(location).inserted_id

            # If the NetCDF file was already created, skip this location.
            if omm_id in forecast.weather_stations:
                continue

            t = threading.Thread(target=wth_series_maker.create_series, args=(omm_id, forecast))
            active_threads.append(t)
            t.start()

        # Wait for the weather grid to be populated.
        for t in active_threads:
            t.join()

        # If the folder is empty, delete it.
        if len(os.listdir(forecast.paths.wth_csv_read)) == 0:
            shutil.rmtree(forecast.paths.wth_csv_read)

        # forecast_id = db.forecasts.insert_one(forecast.persistent_view()).inserted_id
        # forecast['_id'] = forecast_id
        #
        # simulations = []
        #
        # # Flatten simulations and update location info (with id's and computed weather stations).
        # for loc_key, loc_simulations in forecast.simulations.iteritems():
        #     for sim in loc_simulations:
        #         sim.location = forecast.locations[loc_key]
        #         sim.forecast_id = forecast_id
        #         sim.forecast_date = forecast.forecast_date
        #         sim.crop_type = forecast.crop_type
        #         sim.execution_details = DotDict()
        #         simulations.append(sim)
        #
        #         # if forecast.forecast_date in self.synth_values:
        #         #     synth = self.synth_values[forecast.forecast_date]
        #         #     sim.cyclic_results = {
        #         #         'HWAM': []
        #         #     }
        #         #
        #         #     for year in forecast.rainfall[str(sim.location.weather_station)]:
        #         #         if year == '0':
        #         #             continue
        #         #
        #         #         result = {
        #         #             'value': random.uniform(synth['min'], synth['max']),
        #         #             'weather_serie': year
        #         #         }
        #         #         sim.cyclic_results['HWAM'].append(result)
        #
        #         # TODO: habilitar persistencia.
        #         sim_id = db.simulations.insert_one(sim.persistent_view()).inserted_id
        #         sim['_id'] = sim_id
        #
        # forecast.simulations = simulations
        #
        # # Escribir campaña...
        #
        # # Ejecutar simulaciones.
        #
        # # Insertar ID's de simulaciones en el pronóstico.
        # # TODO: habilitar persistencia.
        # db.forecasts.update_one(
        #     {"_id": forecast_id},
        #     {"$pushAll": {
        #         "simulations": [s['_id'] for s in forecast.simulations]
        #     }}
        # )

        logging.getLogger('main').info('Finished running forecast "%s" (time=%s).\n' %
                                       (forecast.name, repr(time.time() - run_start_time)))