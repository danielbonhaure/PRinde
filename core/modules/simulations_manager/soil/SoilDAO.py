import logging
import os
import json
from core.lib.io.file import listdir_fullpath, filename_without_ext

__author__ = 'Federico Schmidt'

soils_path = os.path.join('.', 'data', 'soils')
soils_dict = None


def load_soils():
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

    print('Found %d soils.' % len(soils_dict))

if not soils_dict:
    soils_dict = {}
    load_soils()


class SoilDAO:

    def __init__(self):
        pass

    @staticmethod
    def get_soil(soil_id):
        if not soils_dict:
            raise RuntimeError('Soils dictionary not configured.')

        if soil_id in soils_dict:
            try:
                return json.load(open(soils_dict[soil_id]), encoding='latin-1')
            except Exception as ex:
                print('@ soil file: "%s.json"' % soil_id)
                raise ex
        else:
            logging.warn('Soil "%s" not found in soils directory (%s).' % (soil_id, soils_path))
            return None
