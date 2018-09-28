# coding=utf-8
import os
import yaml
from core.lib.io.file import listdir_fullpath

__author__ = 'Federico Schmidt'

import json
import numpy as np

n_kg = {
    'default': 50  # N inicial
}
horizon_limit = 60  # Profundidad máxima en la cual distribuir el N inicial.
forecast_file = '../config/forecasts/sb_forecast_PY.yaml'

n_err_margin = 0.5
nh4_ppm_first_half = 0.5
nh4_ppm_second_half = 0.3


def calculate_nitrogen(soil_file, n_kg_nitrogen, error_margin):
    soil_layers = soil_file['soils'][0]['soilLayer']

    horizons_lower_limit = [int(l['sllb']) for l in soil_layers]
    horizons_density = [float(l['slbdm']) for l in soil_layers]

    horizons_depth = [horizons_lower_limit[0]]
    curr_depth = horizons_depth[0]
    hrzn_limit_index = 0
    for i in range(1, len(horizons_lower_limit)):
        hrzn_depth = horizons_lower_limit[i] - horizons_lower_limit[i-1]
        curr_depth += hrzn_depth

        if curr_depth <= horizon_limit:
            hrzn_limit_index = i

        horizons_depth.append(hrzn_depth)

    if horizons_lower_limit[hrzn_limit_index] < horizon_limit:
        hrzn_limit_index += 1

    horizons_coeff = (np.array(horizons_depth) * 100) * np.array(horizons_density) / 1000

    def sum_distributed_nitrogen(first_layer_ppm, n_horizons, nitrogen_coefficients):
        layers_ppm = [first_layer_ppm]
        for n_layer in range(1, n_horizons+1):
            layers_ppm.append(layers_ppm[n_layer-1]/2)
        return np.sum(np.array(layers_ppm) * np.array(nitrogen_coefficients[0:n_horizons+1]))

    def make_target_function(n_horizons, nitrogen_coefficients, target_nitrogen, error_margin):
        def f(first_layer_ppm):
            nitrogen_sum = sum_distributed_nitrogen(first_layer_ppm, n_horizons, nitrogen_coefficients)
            nitrogen_error = nitrogen_sum - target_nitrogen
            if abs(nitrogen_error) <= error_margin:
                nitrogen_error = 0
            return nitrogen_error
        return f

    def bisection(f, lower, higher, tolerance=1.):
        assert not (f(lower) * f(higher) > 0)

        middle = (lower + higher) / 2.0
        while (higher - lower) / 2.0 > tolerance:
            if f(middle) == 0:
                return middle
            elif f(lower) * f(middle) < 0:
                higher = middle
            else:
                lower = middle
            middle = (lower + higher) / 2.0
        return middle

    f = make_target_function(hrzn_limit_index, horizons_coeff, n_kg_nitrogen, error_margin)
    first_layer_ppm = bisection(f, 1, 40, tolerance=0.1)

    layers_half_idx = len(soil_layers) // 2 + len(soil_layers) % 2

    layers_nh4 = [nh4_ppm_first_half] * int(layers_half_idx) + [nh4_ppm_second_half] * (len(soil_layers) - int(layers_half_idx))
    layers_no3 = [round(first_layer_ppm - nh4_ppm_first_half, 1)]

    for layer_idx in range(1, len(layers_nh4)):
        layers_no3.append(round(max(layers_no3[layer_idx-1]/2 - layers_nh4[layer_idx], 0.1), 1))

    return (layers_nh4, layers_no3)


soils_initial_values = {}

# for file_name in listdir_fullpath('./salado_soils'):
for file_name in listdir_fullpath('../data/soils/py_soils'):
    if 'json' not in file_name:
        continue
    with open(file_name, mode='r') as f:
        soil_file = json.load(f)

    if file_name in n_kg:
        target_kilograms = n_kg[file_name]
    else:
        target_kilograms = n_kg['default']

    icnh4, icno3 = calculate_nitrogen(soil_file, target_kilograms, n_err_margin)
    soils_initial_values[os.path.basename(file_name)] = {
        'icnh4': icnh4,
        'icno3': icno3
    }

# forecast_file = '/home/federico/Desarrollo/PycharmProjects/PRinde/config/forecasts/mike_she/mz-e_mike_she.yaml'
with open(forecast_file) as f:
    forecast = yaml.safe_load(f)

# Join site_characteristics, initial_conditions and agronomic_managements by location.
for loc_key, values in forecast['initial_conditions'].items():
    if loc_key not in forecast['site_characteristics']:
        raise RuntimeError('Missing location "%s" in site_characteristics.' % loc_key)

    for soil_key, soil_ic in values.items():
        if soil_key not in forecast['site_characteristics'][loc_key]:
            raise RuntimeError('Missing soil "%s" in site_characteristics[%s].' % (soil_key, loc_key))

        soil_id = "%s.json" % forecast['site_characteristics'][loc_key][soil_key]['id']
        if soil_id not in soils_initial_values:
            raise RuntimeError('Soil %s not found.' % soil_id)

        for ic_key in list(soil_ic.keys()):
            if 'nh4' in ic_key:
                soil_ic[ic_key] = soils_initial_values[soil_id]['icnh4']
            if 'no3' in ic_key:
                soil_ic[ic_key] = soils_initial_values[soil_id]['icno3']

yaml.safe_dump(forecast, open('./new_ics.yaml', mode='w'), indent=4)
