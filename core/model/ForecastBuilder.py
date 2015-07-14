# coding=utf-8
import copy
import json
import os
from core.lib.utils.DotDict import DotDict
from core.model.Forecast import Forecast
from core.model.Simulation import Simulation
from jsonschema import validate

__author__ = 'Federico Schmidt'


class ForecastBuilder:

    def __init__(self, forecast_file, schema_path=None):
        self.forecast_file = forecast_file
        self.simulation_schema = None
        if schema_path and os.path.exists(schema_path):
            self.simulation_schema = json.load(open(schema_path, mode='r'))

    def inherit_config(self, system_config=None):
        forecast_file = self.forecast_file

        if 'configuration' not in forecast_file:
            forecast_file['configuration'] = system_config
        else:
            new_config = copy.deepcopy(system_config)
            new_config.update(forecast_file['configuration'])
            forecast_file['configuration'] = new_config

    def replace_alias(self, alias_dict):
        if alias_dict:
            self.__replace_keys__(self.forecast_file, alias_dict)

    def build(self):
        # type(self.forecast_file) = DotDict
        forecast = self.forecast_file

        forecast_list = []

        simulations = DotDict()

        # Join site_characteristics, initial_conditions and agronomic_managements by location.
        joined_locations = DotDict()
        for loc_key, values in forecast['locations'].iteritems():
            joined_loc = DotDict()

            if loc_key not in forecast.site_characteristics:
                raise RuntimeError('Missing site characteristics for location "%s".' % loc_key)

            joined_loc.update(values)
            joined_loc.soils = forecast.site_characteristics[loc_key]

            if loc_key not in forecast.initial_conditions:
                raise RuntimeError('Missing initial conditions for location "%s".' % loc_key)

            if loc_key not in forecast.agronomic_management:
                raise RuntimeError('Missing agronomic management for location "%s".' % loc_key)

            for soil_key in joined_loc.soils:
                if soil_key not in forecast.initial_conditions[loc_key]:
                    raise RuntimeError('Missing initial conditions for soil "%s" at location "%s".' %
                                       (soil_key, loc_key))

                joined_loc.soils[soil_key]['initial_conditions'] = forecast.initial_conditions[loc_key][soil_key]

                if soil_key not in forecast.agronomic_management[loc_key]:
                    raise RuntimeError('Missing agronomic management for soil "%s" at location "%s".' %
                                       (soil_key, loc_key))

                joined_loc.soils[soil_key]['agonomic_management'] = forecast.agronomic_management[loc_key][soil_key]

            joined_locations[loc_key] = joined_loc

        # Create simulations based on complex fields (those that allow more than one value).
        for loc_key, loc in joined_locations.iteritems():
            simulations[loc_key] = []

            # Unwind managements.
            for soil_key, soil in loc.soils.iteritems():
                for mgmt_key, management in soil.agonomic_management.iteritems():
                    sim = DotDict({
                        'location': copy.deepcopy(loc),
                        'soil': copy.deepcopy(soil),
                        'management': copy.deepcopy(management),
                        'initial_conditions': copy.deepcopy(soil['initial_conditions'])
                    })
                    del sim['soil']['agonomic_management']
                    del sim['soil']['initial_conditions']
                    del sim['location']['soils']

                    sim.name = loc.name
                    sim.name += ' - Soil: "%s"' % soil.id

                    if 'mgmt_name' not in management:
                        management.mgmt_name = mgmt_key

                    sim.name += ' - Mgmt: "%s"' % management.mgmt_name

                    ic_water_var = None
                    ic_water_var_content = None

                    if 'ich20_frac' in sim.initial_conditions:
                        ic_water_var = 'ich20_frac'
                        ic_water_var_content = sim.initial_conditions['ich20_frac']

                    if 'frac_full' in sim.initial_conditions:
                        if ic_water_var:
                            raise RuntimeError("Water content can't be defined with two variables.")
                        ic_water_var = 'frac_full'
                        ic_water_var_content = sim.initial_conditions['frac_full']

                    # TODO: ¿puede ic_water_var_content ser None en alguna circunstancia? NO!

                    # Unwind initial conditions.
                    if isinstance(ic_water_var_content, DotDict):
                        for ic_name, ic_values in ic_water_var_content.iteritems():
                            new_sim = copy.deepcopy(sim)
                            new_sim.initial_conditions[ic_water_var] = ic_values
                            new_sim.name += ' - IC: "%s=%s"' % (ic_water_var, ic_name)
                            new_sim.water_content = ic_name

                            if self.simulation_schema:
                                validate(json.loads(new_sim.to_json()), schema=self.simulation_schema)
                            simulations[loc_key].append(Simulation(new_sim))
                    else:
                        if self.simulation_schema:
                            validate(json.loads(sim.to_json()), schema=self.simulation_schema)
                        sim.water_content = ''
                        simulations[loc_key].append(Simulation(sim))

        # Unwind dates and create a forecast for each different date.
        if ('forecast_date' in forecast) and isinstance(forecast['forecast_date'], list):
            for date in forecast['forecast_date']:
                sims = copy.deepcopy(simulations)

                f = Forecast(forecast)
                f.forecast_date = date
                f.simulations = sims
                forecast_list.append(f)
        else:
            f = Forecast(forecast)
            f.simulations = simulations
            forecast_list.append(f)

        return forecast_list

    def __replace_keys__(self, forecast_dict, alias_dict):
        for key, value in forecast_dict.iteritems():
            if isinstance(value, DotDict):
                self.__replace_keys__(value, alias_dict)
            if key in alias_dict:
                forecast_dict[alias_dict[key]] = value
                del forecast_dict[key]
