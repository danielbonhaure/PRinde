import copy
import yaml
from core.lib.utils.DotDict import DotDict

__author__ = 'Federico Schmidt'


class Simulation(DotDict):

    def __init__(self, simulation_yaml):
        super(Simulation, self).__init__()
        self.name = None
        self.init_from_yaml(simulation_yaml)

    def init_from_yaml(self, simulation_yaml):
        if 'name' in simulation_yaml:
            self.name = simulation_yaml['name']

        # Check initial condition variables (soil ic data).
        for soil_ic_var, var_content in simulation_yaml.initial_conditions.iteritems():
            if isinstance(var_content, list):
                # Check that the amount of values matches the amount of soil horizons.
                if len(var_content) != simulation_yaml.soil.n_horizons:
                    raise RuntimeError("The amount of soil horizons found for variable \"%s\" (%d) doesn't match the "
                                       "declared amount of horizons in the soil.n_horizons property (%d) at Simulation "
                                       "\"%s\"." %
                                       (soil_ic_var, len(var_content), simulation_yaml.soil.n_horizons, self.name))

                # Check that every value inside the array is a valid type.
                valid_types = map(lambda x: isinstance(x, (int, long, float)), var_content)
                if not all(valid_types):
                    raise RuntimeError("Values inside variable \"%s\" are not valid types (int, long or float)."
                                       % soil_ic_var)

            elif not isinstance(var_content, (int, long, float)):
                raise RuntimeError("Value for variable \"%s\" is not a valid type (int, long or float).")

        # Update the class dict so we can access values with dot notation.
        self.__dict__.update(simulation_yaml)

    def __repr__(self):
        if self.name:
            return "Simulation <%s>" % self.name
        return "Simulation <%s>" % repr(self.__dict__)

    def to_str(self):
        return yaml.dump(self.__dict__)

    def persistent_view(self):
        view = copy.deepcopy(self.__dict__)
        view['location_id'] = view['location']['_id']
        view['location_name'] = view['location']['name']
        view['soil_id'] = view['soil']['id']
        del view['location']

        if 'weather_station' in view:
            del view['weather_station']

        if 'mgmt_name' in view['management']:
            view['management_name'] = view['management']['mgmt_name']
        else:
            view['management_name'] = 'undefined'
        return view
