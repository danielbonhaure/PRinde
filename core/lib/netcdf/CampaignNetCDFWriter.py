__author__ = 'Federico Schmidt'

from netCDF4 import Dataset


class CampaignNetCDFWriter:

    def __init__(self):
        pass

    @staticmethod
    def join_csv_files(simulation_list, output_file_path):
        nectdf_variables = {}
        output_file = Dataset(output_file_path, 'w')

        pass
