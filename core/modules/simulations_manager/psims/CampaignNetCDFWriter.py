import numpy as np

from core.modules.simulations_manager.soil.SoilDAO import SoilDAO

__author__ = 'Federico Schmidt'

from netCDF4 import Dataset


class CampaignNetCDFWriter:

    def __init__(self):
        pass

    @staticmethod
    def write_campaign(forecast, output_file_path):
        output_file = Dataset(output_file_path, 'w')
        simulation_list = forecast.simulations

        soils = {}
        netcdf_variables = {}

        loc_count = len(simulation_list.values())

        simulations_count = []
        soil_layers_count = []
        scenarios_count = []

        # Gather information to create dimensions.
        for loc_key, loc_simulations in simulation_list.iteritems():
            simulations_count.append(len(loc_simulations))
            for sim in loc_simulations:
                soil_id = sim.soil.id

                if soil_id not in soils:
                    soil = SoilDAO.get_soil(soil_id)
                    soils[soil_id] = soil

                    if not soil:
                        raise RuntimeError('Soil %s not found.' % soil_id)
                else:
                    soil = soils[soil_id]

                len_layers = len(soil['soilLayer'])

                if len_layers != sim.soil.n_horizons:
                    raise RuntimeError('Mismatch between count of soil horizons in forecast specification file (%s)'
                                       ' and soil file (%s) for soil "%s".' %
                                       (len_layers, sim.soil.n_horizons, sim.soil.id))
                sim['initial_conditions']['icbl'] = [int(layer['sllb']) for layer in soil['soilLayer']]
                sim['management']['soil_id'] = soil_id

                soil_layers_count.append(len_layers)
                scenarios_count.append(sim.weather_station['num_scenarios'])

        max_simulation_count = max(simulations_count)
        max_soil_layer_count = max(soil_layers_count)
        n_scenarios = min(scenarios_count)

        cell_width = forecast.configuration.grid_resolution / 60.
        lat_cells_dec = np.arange(90+cell_width, 90-(loc_count-1)*cell_width, -cell_width)
        lon_cells_dec = np.arange(-180-cell_width, -180+(max_simulation_count-1)*cell_width, cell_width)

        dim_sizes = [loc_count, max_simulation_count, n_scenarios, max_soil_layer_count]
        dim_var_contents = [lat_cells_dec, lon_cells_dec, np.arange(0, n_scenarios), np.arange(0, max_soil_layer_count)]

        # Create dimensions and associated variables.
        for dim_idx, dim in enumerate(['lat', 'lon', 'scen', 'soil_layer']):
            output_file.createDimension(dim, size=dim_sizes[dim_idx])
            dim_var = output_file.createVariable(varname=dim, datatype='f8', dimensions=(dim,), fill_value=-99)
            dim_var[:] = dim_var_contents[dim_idx]

        wst_id = output_file.createVariable(varname='wst_id', datatype='u2', dimensions=('scen',), fill_value=-99)
        wst_id[:] = range(0, n_scenarios)

        for loc_idx, loc_simulations in enumerate(simulation_list.values()):
            for sim_idx, sim in enumerate(loc_simulations):
                sim.lat_idx = loc_idx
                sim.lon_idx = sim_idx

                for var, content in sim['initial_conditions'].iteritems():
                    if var not in netcdf_variables:
                        netcdf_var = output_file.createVariable(varname=var, datatype='f8',
                                                                dimensions=('lat', 'lon', 'soil_layer'),
                                                                fill_value=-99)
                        netcdf_variables[var] = netcdf_var
                    else:
                        netcdf_var = netcdf_variables[var]
                    netcdf_var[sim.lat_idx, sim.lon_idx, 0:len(content)] = content

                for var, content in sim['management'].iteritems():
                    if var == 'mgmt_name':
                        continue

                    datatype = 'f8'
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
