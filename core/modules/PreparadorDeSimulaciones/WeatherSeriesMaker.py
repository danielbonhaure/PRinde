__author__ = 'Federico Schmidt'

from core.lib.io.file import listdir_fullpath
import os.path
import csv

class WeatherSeriesMaker:

    def __init__(self, system_config):
        self.system_config = system_config

    def create_from_db(self, omm_id):
        # TODO: no hardcodear.
        return os.path.join(self.system_config.root_path, ".tmp", "wth_db", str(omm_id))

    def create_series(self, omm_id):
        path = self.create_from_db(omm_id)

        # List the directory where the CSV files with the weather information are.
        dir_list = listdir_fullpath(path, onlyFiles=True, filter=(lambda x: x.endswith('csv')))

        # Build the main file path (the file with the station information).
        main_file = os.path.join(path, ('_' + str(omm_id) + '.csv'))
        print(main_file)

        for f in dir_list:
            f = open(f)

            if f.name == main_file:
                info = self.read_station_info(f)
                print(info)
            print f.name
        pass

    def read_station_info(self, csv_file):
        csv_file = csv.reader(csv_file, delimiter='\t')

        header = csv_file.next()
        row = csv_file.next()

        info = dict()

        for index, item in enumerate(header):
            info[item] = row[index]

        return(info)