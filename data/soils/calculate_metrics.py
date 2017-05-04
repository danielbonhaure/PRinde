import json
import logging
import os
from core.lib.io.file import listdir_fullpath, filename_without_ext
from pymongo import MongoClient
__author__ = 'Federico Schmidt'


# soils_path = os.path.join('.', 'data', 'soils')
soils_path = os.path.join('.')
soils_dict = {}

mongo_connection = MongoClient('mongodb://localhost:27019')
collection = mongo_connection['Rinde']['soils']

def find_soils():
    def is_soil_file(x):
        return os.path.splitext(x)[1].lower() == '.json'

    soil_files = listdir_fullpath(soils_path, recursive=True, onlyFiles=True, filter=is_soil_file)

    for f in soil_files:
        key = filename_without_ext(f)

        if key in soils_dict:
            logging.warn('Duplicated soil name "%s". Found at two different paths: "%s" and "%s".' % (key,
                                                                                                      soils_dict[key],
                                                                                                      f))
            continue

        soils_dict[key] = f

find_soils()

def calculate_metrics(soil_layers):
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

metrics_array = []

for soil_name, soil_file_name in soils_dict.iteritems():
    with open(soil_file_name, mode='r') as soil_file:
        soil_json = json.load(soil_file, encoding='latin-1')['soils'][0]
        soil_id = soil_json['soil_id']
        soil_layers = soil_json['soilLayer']
        soil_metrics = calculate_metrics(soil_layers)

        metrics_array.append({
            '_id': soil_id,
            'metrics': soil_metrics
        })

collection.insert_many(metrics_array)
