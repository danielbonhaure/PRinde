# -*- coding: utf-8 -*-
import os
import shutil
import threading
from datetime import datetime, timedelta
import logging
from core.lib.io.file import create_folder_with_permissions
from core.lib.utils.log import log_format_exception
from core.modules.simulations_manager.CampaignWriter import CampaignWriter
from core.lib.utils.extended_collections import DotDict
from core.modules.simulations_manager.CombinedSeriesMaker import CombinedSeriesMaker
from core.modules.simulations_manager.HistoricalSeriesMaker import HistoricalSeriesMaker
from core.modules.simulations_manager.RunpSIMS import RunpSIMS
import time
import copy
from lib.jobs.monitor import NullMonitor, JOB_STATUS_WAITING, ProgressMonitor

__author__ = 'Federico Schmidt'


class ForecastManager:

    def __init__(self, scheduler, system_config):
        self.system_config = system_config
        self.psims_runner = RunpSIMS(self.system_config.jobs_lock)
        self.scheduler = scheduler

    def start(self):
        for file_name, forecast_list in self.system_config.forecasts.iteritems():
            for forecast in forecast_list:
                job_name = "%s (%s)" % (forecast.name, forecast.forecast_date)
                run_date = datetime.strptime(forecast.forecast_date, '%Y-%m-%d')

                # TODO: remove this.
                # run_date = datetime.now() + timedelta(days=1)

                if run_date < datetime.now():
                    # Run immediately.
                    job_handle = self.scheduler.add_job(self.run_forecast, name=job_name, args=[forecast])
                    forecast['job_id'] = job_handle.id
                else:
                    # Schedule for running at the specified date at 19:00.
                    run_date = run_date.replace(hour=19)

                    # scheduler.add_job(Worker.create_and_run, 'interval', seconds=5, args=[job_0, scheduler])
                    job_handle = self.scheduler.add_job(self.run_forecast, trigger='date', name=job_name, args=[forecast],
                                                        run_date=run_date)
                    forecast['job_id'] = job_handle.id

    def __run__(self):
        # Create scheduler, etc.
        for forecast in self.system_config.forecasts:
            self.run_forecast(forecast)

    def reschedule_forecast(self, forecast):
        job_name = "Rescheduled: %s (%s)" % (forecast.name, forecast.forecast_date)
        new_run_date = datetime.now() + datetime.timedelta(days=1)
        job_handle = self.scheduler.add_job(self.run_forecast, trigger='date', name=job_name, args=[forecast],
                                            run_date=new_run_date)
        forecast['job_id'] = job_handle.id

    def run_forecast(self, forecast, progress_monitor=None):
        logging.getLogger().info('\nRunning forecast "%s" (%s).' % (forecast.name, forecast.forecast_date))

        psims_exit_code = None
        db = None
        forecast_id = None
        simulations_ids = None

        if not progress_monitor:
            progress_monitor = NullMonitor

        progress_monitor.job_started()

        time.sleep(5)
        progress_monitor.update_progress(new_value=5, job_status=JOB_STATUS_WAITING)
        time.sleep(15)
        spm1 = ProgressMonitor()
        spm2 = ProgressMonitor()
        progress_monitor.add_subjob(subjob_progress_monitor=spm1, job_name='Sub Job 1')
        progress_monitor.add_subjob(subjob_progress_monitor=spm2, job_name='Sub Job 2')
        spm1.job_started()
        time.sleep(5)
        spm2.job_started()
        time.sleep(5)
        spm1.update_progress(job_status=JOB_STATUS_WAITING)
        spm2.update_progress(new_value=10)
        time.sleep(6)
        spm2.update_progress(new_value=45)
        spm1.update_progress(15)
        time.sleep(7)
        spm1.job_ended()
        time.sleep(3)
        spm2.job_ended()
        time.sleep(8)
        progress_monitor.update_progress(45)
        time.sleep(6)
        progress_monitor.update_progress(57)
        time.sleep(5)
        progress_monitor.update_progress(82)
        time.sleep(7)
        progress_monitor.job_ended()

        return

        try:
            forecast = copy.deepcopy(forecast)
            run_start_time = time.time()

            # Get MongoDB connection.
            db = self.system_config.database['rinde_db']

            wth_series_maker = None
            forecast.configuration['simulation_collection'] = 'simulations'

            if forecast.configuration.weather_series == 'combined':
                wth_series_maker = CombinedSeriesMaker(self.system_config, forecast.configuration.max_paralellism)
            elif forecast.configuration.weather_series == 'historic':
                wth_series_maker = HistoricalSeriesMaker(self.system_config, forecast.configuration.max_paralellism)
                forecast.configuration['simulation_collection'] = 'reference_simulations'
            else:
                raise RuntimeError('Weather series type "%s" unsupported.' % forecast.configuration.weather_series)

            folder_name = "%s - %s" % (datetime.now().isoformat(), forecast['name'])
            forecast.folder_name = folder_name

            # Add folder name to rundir and create it.
            forecast.paths.rundir = os.path.abspath(os.path.join(forecast.paths.rundir, folder_name))
            create_folder_with_permissions(forecast.paths.rundir)

            # Create a folder for the weather grid inside that rundir.
            forecast.paths.weather_grid_path = os.path.join(forecast.paths.rundir, 'wth')
            create_folder_with_permissions(forecast.paths.weather_grid_path)

            # Create a folder for the soil grid inside that rundir.
            forecast.paths.soil_grid_path = os.path.join(forecast.paths.rundir, 'soils')
            create_folder_with_permissions(forecast.paths.soil_grid_path)

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
                    # "coord_x": location.coord_x,
                    # "coord_y": location.coord_y,
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

                t = threading.Thread(target=wth_series_maker.create_series, name='create_series for omm_id = %s' %
                                                                                 omm_id, args=(omm_id, forecast))
                active_threads.append(t)
                t.start()

            # Wait for the weather grid to be populated.
            for t in active_threads:
                t.join()

            # If the folder is empty, delete it.
            if len(os.listdir(forecast.paths.wth_csv_read)) == 0:
                shutil.rmtree(forecast.paths.wth_csv_read)

            forecast_persistent_view = forecast.persistent_view()
            forecast_id = None
            if forecast_persistent_view:
                forecast_id = db.forecasts.insert_one(forecast_persistent_view).inserted_id
                forecast['_id'] = forecast_id

            simulations_ids = []

            # Flatten simulations and update location info (with id's and computed weather stations).
            for loc_key, loc_simulations in forecast.simulations.iteritems():
                for sim in loc_simulations:
                    sim.location = forecast.locations[loc_key]
                    sim.weather_station = forecast.weather_stations[sim.location.weather_station]
                    if forecast_id:
                        sim.forecast_id = forecast_id
                        sim.forecast_date = forecast.forecast_date
                    sim.crop_type = forecast.crop_type
                    sim.execution_details = DotDict()

                    # If a simulation has an associated forecast, it should go inside the 'simulations' collection.
                    if forecast_id:
                        sim_id = db.simulations.insert_one(sim.persistent_view()).inserted_id
                    else:
                        # Otherwise, it means it's a reference (historic) simulation.
                        sim_id = db.reference_simulations.insert_one(sim.persistent_view()).inserted_id
                    sim['_id'] = sim_id
                    simulations_ids.append(sim_id)

            forecast.paths.run_script_path = CampaignWriter.write_campaign(forecast, output_dir=forecast.paths.rundir)
            forecast.simulation_count = len(simulations_ids)

            # Insertar ID's de simulaciones en el pronóstico.
            if forecast_id:
                db.forecasts.update_one(
                    {"_id": forecast_id},
                    {"$pushAll": {
                        "simulations": simulations_ids
                    }}
                )

            # Ejecutar simulaciones.
            psims_exit_code = self.psims_runner.run(forecast, verbose=True)

            logging.getLogger().info('Finished running forecast "%s" (time=%s).\n' %
                                     (forecast.name, repr(time.time() - run_start_time)))
        except:
            logging.getLogger().error("Failed to run forecast '%s'. Reason: %s" %
                                      (forecast.name, log_format_exception()))
        finally:
            if psims_exit_code != 0:
                if db:
                    if simulations_ids and len(simulations_ids) > 0:
                        db[forecast.configuration['simulation_collection']].delete_many(
                            {"_id": {"$in": simulations_ids}}
                        )
                    if forecast_id:
                        db.forecasts.delete_one({"_id": forecast_id})

                # Reschedule (and notify?)
                pass
            else:
                # Clean the rundir.
                shutil.rmtree(forecast.paths.rundir)
                # Check simulation results.
                pass
