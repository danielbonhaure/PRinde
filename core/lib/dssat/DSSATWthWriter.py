from datetime import date, datetime
import os
import csv
import numpy as np
from core.modules.simulations_manager.weather.csv.CSVDatabaseWeatherSeries import CSVDatabaseWeatherSeries

__author__ = 'Federico Schmidt'


class DSSATWthWriter:

    expected_variables = ['fecha', 'rad', 'tmax', 'tmin', 'prcp', 'fecha_original']
    expected_var_set = set(expected_variables)

    def __init__(self):
        pass

    @staticmethod
    def join_csv_files(dir_list, output_file_path, extract_rainfall=True, forecast_date=None, location=None):
        csv_files = dir_list

        rainfall_data = None
        if extract_rainfall:
            rainfall_data = dict()

        for i, csv_file in enumerate(csv_files):
            csv_reader = csv.reader(open(csv_file), delimiter='\t')

            csv_content = DSSATWthWriter.write_wth_file(i, csv_reader, output_file_path, location)

            # # change permissions
            # f = os.open(filename, os.O_RDONLY)
            # os.fchmod(f, stat.S_IREAD | stat.S_IWRITE | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH)
            # os.close(f)

            # Check if we need to extract rainfall data.
            if extract_rainfall:
                scen_name = str(CSVDatabaseWeatherSeries.__scen_name__(csv_file))
                DSSATWthWriter.extract_rainfall(rainfall_data, csv_content, forecast_date, scen_name)

        return True, rainfall_data

    @staticmethod
    def write_wth_file(scenario_index, file_rows_content, output_file_path, location):
        lat = float(location['coord_y'])
        lon = float(location['coord_x'])

        filler, filler2 = -99, 10  # used for ELEV, REFHT, WNDHT, respectively
        var_names = ['SRAD', 'TMAX', 'TMIN', 'RAIN']

        csv_content = dict()
        csv_variables = []
        months = []

        for r_index, row in enumerate(file_rows_content):
                if r_index == 0:
                    # Header
                    csv_variables = row

                    # Check that the header variables match the expected variables.
                    if len(DSSATWthWriter.expected_var_set.intersection(csv_variables)) != \
                            len(DSSATWthWriter.expected_variables):
                        raise RuntimeError("The variables in the weather stream (%s) don't match the expected ones (%s)"
                                           "." % (csv_variables, DSSATWthWriter.expected_variables))

                    for column in row:
                        csv_content[column] = []

                else:
                    for v, value in enumerate(row):
                        var_name = csv_variables[v]

                        if var_name not in DSSATWthWriter.expected_variables:
                            continue

                        if var_name == 'fecha':
                            dt = value
                            if not isinstance(value, date):
                                dt = datetime.strptime(value, '%Y-%m-%d')
                            csv_content[var_name].append(int(dt.strftime("%y%j")))
                            months.append(dt.month)
                        elif var_name == 'fecha_original':
                            dt = value
                            if not isinstance(value, date):
                                dt = datetime.strptime(value, '%Y-%m-%d')
                            csv_content[var_name].append(int(dt.strftime("%Y%m%d")))
                        else:
                            csv_content[var_name].append(float(value))

        nt = len(csv_content['fecha'])
        tmin, tmax = np.array(csv_content['tmin']), np.array(csv_content['tmax'])
        months = np.array(months)
        tav = float(0.5 * (tmin.sum() + tmax.sum()) / nt)  # function of scen

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

        filename = os.path.join(output_file_path, ('WTH' + str(scenario_index).zfill(5) + '.WTH'))

        # write header
        head = '*WEATHER DATA : ' + str(location) + '\n'
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

        return csv_content

    @staticmethod
    def extract_rainfall(rainfall_data, csv_content, forecast_date, scen_name):
        # Calculate forecast date as time difference (in days).
        if forecast_date:
            forecast_date = datetime.strptime(forecast_date, '%Y-%m-%d')
            forecast_date = int(forecast_date.strftime("%y%j"))

        rain_variable = np.array(csv_content['prcp'])
        # Convert time variable to Numpy array, otherwise we can't use array indexes.
        time_var_content = np.array(csv_content['fecha'])

        if '0' not in rainfall_data:
            # Extract rainfall data until the date of forecast.
            pre_forecast_time = time_var_content[time_var_content <= forecast_date]
            rain = rain_variable[0:len(pre_forecast_time)]
            rainy_days = np.where(rain > 0)[0]

            rainfall_data['0'] = {
                'dates': pre_forecast_time[rainy_days].tolist(),
                'values': rain[rainy_days].tolist()
            }

        post_forecast_time = time_var_content[time_var_content > forecast_date]
        post_forecast_start = len(time_var_content) - len(post_forecast_time)
        rain = rain_variable[post_forecast_start:]

        rainy_days = np.where(rain > 0)[0]
        rainfall_data[str(scen_name)] = {
            'dates': post_forecast_time[rainy_days].tolist(),
            'values': rain[rainy_days].tolist()
        }
