import os
import json

__author__ = 'Federico Schmidt'


class SoilDAO:

    def __init__(self):
        pass

    @staticmethod
    def get_soil(soil_id):
        file_path = os.path.join('.', 'data', 'soils', soil_id+'.json')
        if os.path.isfile(file_path):
            try:
                return json.load(open(file_path), encoding='latin-1')
            except Exception, ex:
                print('@ soil file: "%s.json"' % soil_id)
                raise ex
        return None
