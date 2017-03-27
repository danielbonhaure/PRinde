from core.lib.jobs.base import BaseJob
from core.lib.jobs.monitor import ProgressMonitor, JOB_STATUS_WAITING, JOB_STATUS_RUNNING
from core.modules.config.priority import UPDATE_DB_DATA
import json, logging

from core.modules.simulations_manager.soil.SoilDAO import soils_dict

__author__ = 'Daniel Bonhaure'


class SoilsUpdater(BaseJob):

    def __init__(self, system_config):
        super(SoilsUpdater, self).__init__(progress_monitor=ProgressMonitor())
        self.system_config = system_config
        self.db = system_config.database['yield_db']

    def run(self):
        self.progress_monitor.start_value = 0
        self.progress_monitor.end_value = len(soils_dict)
        self.progress_monitor.update_progress(job_status=JOB_STATUS_WAITING)

        # Acquire a blocking job lock with the update database priority
        with self.system_config.jobs_lock.blocking_job(priority=UPDATE_DB_DATA):
            # Lock acquired, notify observers.
            self.progress_monitor.update_progress(job_status=JOB_STATUS_RUNNING)

            for pm_actual_value, (soil_name, soil_file_name) in enumerate(soils_dict.iteritems(),1):
                with open(soil_file_name, mode='r') as soil_file:
                    soil_json = json.load(soil_file, encoding='latin-1')['soils'][0]
                    soil_id = soil_json['soil_id']
                    soil_layers = soil_json['soilLayer']
                    soil_metrics = self.calculate_metrics(soil_layers)
                    # Update (or insert) soils.
                    self.db['soils'].update_one({'_id':soil_id}, {'$set':{'metrics': soil_metrics}}, upsert=True)
                    # Update progress information.
                    self.progress_monitor.update_progress(new_value=pm_actual_value)

    def calculate_metrics(self, soil_layers):
        prev_layer_depth = 0
        wilting_point = 0
        field_capacity = 0
        field_saturation = 0

        for layer in soil_layers:
            layer_depth = int(layer['sllb']) * 10
            layer_depth_diff = layer_depth - prev_layer_depth

            wilting_point += float(layer['slll']) * layer_depth_diff
            field_capacity += float(layer['sldul']) * layer_depth_diff
            field_saturation += float(layer['slsat']) * layer_depth_diff

            prev_layer_depth = layer_depth

        return {
            'wilting_point': wilting_point,
            'field_capacity': field_capacity,
            'field_saturation': field_saturation,
            'max_available_water': field_capacity - wilting_point
        }
