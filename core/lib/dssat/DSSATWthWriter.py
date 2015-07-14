import numpy as np
from datetime import datetime
import os
import stat
import csv

__author__ = 'Federico Schmidt'


class DSSATWthWriter:

    expected_variables = ['fecha', 'rad', 'tmax', 'tmin', 'prcp', 'fecha_original']
    expected_var_set = set(expected_variables)

    def __init__(self):
        pass

    @staticmethod
    def join_csv_files(dir_list, output_file_path, extract_rainfall=True, forecast_date=None, station_data=None):
        # Sort the list of files by name.
        csv_files = sorted(dir_list)

        ns = len(csv_files)
        lat = float(station_data['lat_dec'])
        lon = float(station_data['lon_dec'])

        filler, filler2 = -99, 10  # used for ELEV, REFHT, WNDHT, respectively
        var_names = ['SRAD', 'TMAX', 'TMIN', 'RAIN']

        for i, csv_file in enumerate(csv_files):
            csv_reader = csv.reader(open(csv_file), delimiter='\t')

            csv_variables = []
            csv_content = dict()

            months = []

            for r_index, row in enumerate(csv_reader):
                if r_index == 0:
                    # Header
                    csv_variables = row

                    # Check that the header variables match the expected variables.
                    if len(DSSATWthWriter.expected_var_set.intersection(csv_variables)) != \
                            len(DSSATWthWriter.expected_variables):
                        raise RuntimeError("The variables in the CSV file \"%s\" don't match the expected ones (%s)." %
                                           (csv_file, DSSATWthWriter.expected_variables))

                    for column in row:
                        csv_content[column] = []

                else:
                    for v, value in enumerate(row):
                        var_name = csv_variables[v]

                        if var_name not in DSSATWthWriter.expected_variables:
                            continue

                        if var_name == 'fecha':
                            dt = datetime.strptime(value, '%Y-%m-%d')
                            csv_content[var_name].append(int(dt.strftime("%y%j")))
                            months.append(dt.month)
                        elif var_name == 'fecha_original':
                            dt = datetime.strptime(value, '%Y-%m-%d')
                            csv_content[var_name].append(int(dt.strftime("%Y%m%d")))
                        else:
                            csv_content[var_name].append(float(value))

            nt = len(csv_content['fecha'])
            tmin, tmax = np.array(csv_content['tmin']), np.array(csv_content['tmax'])
            months = np.array(months)
            tav = 0.5 * (tmin.sum() + tmax.sum()) / nt  # function of scen

            # compute amp
            month_averages = []
            for j in range(1, 13):
                month_indexes = np.where(months == j)[0]
                t = 0.5 * (tmin[month_indexes].sum() + tmax[month_indexes].sum()) / len(month_indexes)
                month_averages.append(t)
            amp = max(month_averages) - min(month_averages)

            data = None
            for index, v in enumerate(DSSATWthWriter.expected_variables):
                if index == 0:
                    data = (csv_content[v], )
                    continue
                data = data + (csv_content[v], )
            data = np.column_stack(data)

            filename = os.path.join(output_file_path, ('WTH' + str(i).zfill(5) + '.WTH'))

            # write header
            head = '*WEATHER DATA : ' + os.path.basename(csv_file) + '\n'
            head += '@ INSI      LAT     LONG  ELEV   TAV   AMP REFHT WNDHT\n    CI'
            head += '%9.3f' % lat
            head += '%9.3f' % lon
            head += '%6d' % filler + '%6.1f' % tav + '%6.1f' % amp
            head += '%6d' % filler + '%6d' % filler2 + '\n'
            head += '@DATE' + ''.join(['%6s' % v for v in var_names]) + '\n'

            # write body
            with open(filename, 'w') as f:
                f.write(head)
                np.savetxt(f, data, fmt=['%.5d'] + ['%6.1f'] * len(var_names) + ['     !%2.d'], delimiter='')

            # change permissions
            f = os.open(filename, os.O_RDONLY)
            os.fchmod(f, stat.S_IREAD | stat.S_IWRITE | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH)
            os.close(f)

        return True, None
