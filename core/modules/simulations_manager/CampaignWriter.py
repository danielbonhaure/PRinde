import json
import os
import numpy as np

from core.lib.io.file import create_folder_with_permissions
from core.modules.simulations_manager.soil.SoilDAO import SoilDAO

__author__ = 'Federico Schmidt'

from netCDF4 import Dataset


class CampaignWriter:

    def __init__(self):
        pass

    @staticmethod
    def write_campaign(forecast, output_dir):
        netcdf_file_path = os.path.join(output_dir, 'campaign.nc4')
        params_file_path = os.path.join(output_dir, 'params')
        exp_template_path = os.path.join(output_dir, 'exp_template.json')
        run_sh_path = os.path.join(output_dir, 'run_psims.sh')

        sim_stats, gridlist_file_path = CampaignWriter.create_grids(forecast, output_dir)

        CampaignWriter.write_campagin_netcdf(forecast, netcdf_file_path, sim_stats)

        psims_path = os.path.abspath(forecast.paths.psims)
        root_path = forecast.paths.rundir

        res_variables = set(forecast.results.cycle)
        if 'ADAT' in res_variables or 'MDAT' in res_variables:
            res_variables.add('PDAT')
        variables = ','.join(res_variables)

        ref_year = forecast.campaign_start_date.year
        if forecast.configuration.weather_series == 'historic':
            ref_year = 1950

        delta = forecast.configuration.grid_resolution
        out_collection_name = forecast.configuration['simulation_collection']

        n_scens = sim_stats['n_scenarios']
        # Get any simulation (for instance, the first one) and extract names from there.
        # scen_names = sorted(forecast.simulations.values()[0][0].weather_station['scen_names'][0:n_scens])
        # scen_names = str(scen_names).replace(' ', '')

        params_file = os.path.join('.', 'data', 'templates', 'params_template')
        with open(params_file, 'r') as pfile:
            params = pfile.read()

            with open(params_file_path, 'w') as rundir_params:
                rundir_params.write(params % (
                    psims_path,
                    variables,
                    ref_year,
                    delta,
                    n_scens,
                    out_collection_name
                ))

        with open(os.path.join('.', 'data', 'templates', 'run_psims.sh')) as sh_template:
            sh_script = sh_template.read()

            with open(run_sh_path, mode='w') as run_sh:
                run_sh.write(sh_script % (
                    psims_path,
                    root_path
                ))

        crop_template_path = os.path.abspath(os.path.join('.', 'data', 'templates', forecast.crop_template))
        os.symlink(crop_template_path, exp_template_path)

        forecast.paths.campaign_path = os.path.dirname(netcdf_file_path)
        forecast.paths.params_path = params_file_path
        forecast.paths.gridlist_path = gridlist_file_path

        return run_sh_path

    @staticmethod
    def create_grids(forecast, output_dir):
        gridlist_file_path = os.path.join(output_dir, 'gridList.txt')
        simulation_list = forecast.simulations

        soils = {}

        simulations_count = []
        soil_layers_count = []
        scenarios_count = []

        gridlist_file_content = ''
        simulations_data = []

        # Gather information to create dimensions.
        for loc_idx, loc_simulations in enumerate(simulation_list.values()):
            loc_simulations_data = []
            simulations_count.append(len(loc_simulations))
            for sim_idx, sim in enumerate(loc_simulations):
                sim.lat_idx = loc_idx
                sim.lon_idx = sim_idx

                soil_id = sim.soil.id

                soil_json_path = os.path.join(forecast.paths.soil_grid_path, soil_id+'.json')

                gridlist_file_content += '%03d/%03d\n' % (sim.lat_idx, sim.lon_idx)

                if soil_id not in soils:
                    soil = SoilDAO.get_soil(soil_id)
                    soils[soil_id] = soil

                    if not soil:
                        raise RuntimeError('Soil %s not found.' % soil_id)
                    else:
                        json.dump(soil, open(soil_json_path, mode='w'), ensure_ascii=True, indent=4)
                else:
                    soil = soils[soil_id]

                soil_lat_folder = os.path.join(forecast.paths.soil_grid_path, '%03d' % sim.lat_idx)
                soil_lon_folder = os.path.join(soil_lat_folder, '%03d' % sim.lon_idx)

                if not os.path.exists(soil_lat_folder):
                    create_folder_with_permissions(soil_lat_folder)

                if not os.path.exists(soil_lon_folder):
                    create_folder_with_permissions(soil_lon_folder)

                # Create symlink.
                os.symlink(soil_json_path, os.path.join(soil_lon_folder, 'soil.json'))

                wth_lat_folder = os.path.join(forecast.paths.weather_grid_path, '%03d' % sim.lat_idx)
                wth_lon_folder = os.path.join(wth_lat_folder, '%03d' % sim.lon_idx)

                if not os.path.exists(wth_lat_folder):
                    create_folder_with_permissions(wth_lat_folder)

                if os.path.exists(wth_lon_folder):
                    raise RuntimeError('Couldn\t create symlink at "%s", folder already exists.' % wth_lon_folder)

                weather_folder_path = os.path.join(forecast.paths.rundir, sim.weather_station['weather_path'])

                os.symlink(weather_folder_path, wth_lon_folder)

                # Get the first soil (we're working only with one soil per file).
                soil = soil['soils'][0]
                len_layers = len(soil['soilLayer'])

                if len_layers != sim.soil.n_horizons:
                    raise RuntimeError('Mismatch between count of soil horizons in forecast specification file (%s)'
                                       ' and soil file (%s) for soil "%s".' %
                                       (sim.soil.n_horizons, len_layers, sim.soil.id))
                sim['initial_conditions']['icbl'] = [int(layer['sllb']) for layer in soil['soilLayer']]
                sim['management']['soil_id'] = soil_id

                soil_layers_count.append(len_layers)
                scenarios_count.append(sim.weather_station['num_scenarios'])
                loc_simulations_data.append({
                    'id': str(sim['_id']),
                    'scen_names': sim.weather_station['scen_names']
                })

            simulations_data.append(loc_simulations_data)

        # Write the gridlist text file.
        with open(gridlist_file_path, mode='w') as f:
            f.write(gridlist_file_content)

        sim_data = {
            "simulations": simulations_data,
            "oids": False
        }
        json.dump(sim_data, open(os.path.join(forecast.paths.rundir, 'sim_data.json'), 'w'), ensure_ascii=False, indent=4)

        sim_stats = {
            'max_simulation_count': max(simulations_count),
            'max_soil_layer_count': max(soil_layers_count),
            'n_scenarios': max(scenarios_count)
        }
        return sim_stats, gridlist_file_path

    @staticmethod
    def write_campagin_netcdf(forecast, output_file_path, sim_stats):
        output_file = Dataset(output_file_path, 'w')
        simulation_list = forecast.simulations

        netcdf_variables = {}

        loc_count = len(simulation_list.values())

        max_simulation_count = sim_stats['max_simulation_count']
        max_soil_layer_count = sim_stats['max_soil_layer_count']
        n_scenarios = sim_stats['n_scenarios']

        cell_width = forecast.configuration.grid_resolution / 60.
        lat_cells_dec = np.arange(90+cell_width, 90-(loc_count-1)*cell_width, -cell_width)
        lon_cells_dec = np.arange(-180-cell_width, -180+(max_simulation_count-1)*cell_width, cell_width)

        dim_sizes = [loc_count, max_simulation_count, n_scenarios, max_soil_layer_count]
        dim_var_contents = [lat_cells_dec, lon_cells_dec, np.arange(0, n_scenarios), np.arange(0, max_soil_layer_count)]

        # Create dimensions and associated variables.
        for dim_idx, dim in enumerate(['lat', 'lon', 'scen', 'soil_layer']):
            output_file.createDimension(dim, size=dim_sizes[dim_idx])
            dim_var = output_file.createVariable(varname=dim, datatype='f4', dimensions=(dim,), fill_value=-99)
            dim_var[:] = dim_var_contents[dim_idx]

        wst_id = output_file.createVariable(varname='wst_id', datatype='u2', dimensions=('scen',), fill_value=-99)
        wst_id[:] = range(0, n_scenarios)

        for loc_simulations in simulation_list.values():
            for sim in loc_simulations:

                for var, content in sim['initial_conditions'].iteritems():
                    if var not in netcdf_variables:
                        datatype = 'f4'
                        if var == 'icbl':
                            datatype = 'u2'
                        netcdf_var = output_file.createVariable(varname=var, datatype=datatype,
                                                                dimensions=('lat', 'lon', 'soil_layer'),
                                                                fill_value=-99)
                        netcdf_variables[var] = netcdf_var
                    else:
                        netcdf_var = netcdf_variables[var]
                    netcdf_var[sim.lat_idx, sim.lon_idx, 0:len(content)] = content

                for var, content in sim['management'].iteritems():
                    if var == 'mgmt_name':
                        continue

                    datatype = 'f4'
                    if isinstance(content, str):
                        datatype = 'u2'

                    if var not in netcdf_variables:
                        netcdf_var = output_file.createVariable(varname=var, datatype=datatype,
                                                                dimensions=('lat', 'lon'),
                                                                fill_value=-99)
                        netcdf_variables[var] = netcdf_var
                    else:
                        netcdf_var = netcdf_variables[var]

                    if isinstance(content, str):
                        netcdf_var.units = 'Mapping'
                        if 'long_name' not in netcdf_var.ncattrs():
                            netcdf_var.long_name = content
                            # pSIMS indexes for mapping start at 1.
                            content = 1
                        else:
                            var_mapping = netcdf_var.long_name.split(',')
                            if content not in var_mapping:
                                var_mapping.append(content)
                                content = len(var_mapping)
                                # Workaround for Python NetCDF lib bug (delete var attr first, then write it again).
                                del netcdf_var.long_name
                                netcdf_var.long_name = ','.join(var_mapping)
                            else:
                                # Find the position of 'content' inside the 'long_name' attribute.
                                content = var_mapping.index(content) + 1  # (+1 since pSIMS Mapping indexes start at 1)

                    netcdf_var[sim.lat_idx, sim.lon_idx] = content

        output_file.close()
