import copy
from xxhash import xxh64
from core.lib.utils.extended_collections import DotDict

__author__ = 'Federico Schmidt'


class Location(DotDict):

    def __init__(self, location_yaml, forecast, system_config):
        super(Location, self).__init__()
        self.init_from_yaml(location_yaml, forecast, system_config)
        self.id = xxh64('%s,%s,%s,%s' % (self.name.encode('utf-8'), self.weather_station, self.coord_x, self.coord_y)).hexdigest()

    def init_from_yaml(self, loc_yaml, forecast, system_config):
        if 'name' not in loc_yaml:
            loc_yaml['name'] = repr(loc_yaml)

        # Perform the check and modifications that the weather series maker requires.
        loc_yaml = forecast.configuration.weather_maker_class.validate_location(loc_yaml, forecast, system_config)

        self.update(loc_yaml)

    def persistent_view(self):
        v = copy.deepcopy(self.__dict__)
        del v['id']
        return v
