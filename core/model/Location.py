import logging
from xxhash import xxh64
from core.lib.utils.extended_collections import DotDict

__author__ = 'Federico Schmidt'


class Location(DotDict):

    def __init__(self, location_yaml, weather_db_connection):
        super(Location, self).__init__()
        self.init_from_yaml(location_yaml, weather_db_connection)
        self.id = xxh64('%s,%s,%s,%s' % (self.name.encode('utf-8'), self.weather_station, self.coord_x, self.coord_y)).hexdigest()

    def init_from_yaml(self, loc_yaml, weather_db_connection):
        name = repr(loc_yaml)
        if 'name' in loc_yaml:
            name = loc_yaml['name']

        if 'weather_station' not in loc_yaml or len(str(loc_yaml['weather_station'])) == 0:
            # Perform weather station lookup.
            if 'coord_x' not in loc_yaml or 'coord_y' not in loc_yaml:
                raise RuntimeError('No weather station ID or coordinates provided for location "%s".' % name)

            try:
                coord_x = float(loc_yaml['coord_x'])
                coord_y = float(loc_yaml['coord_y'])
            except Exception:
                raise RuntimeError('Failed to parse coordinates to float for location "%s".' % name)

            nearest_station_query = """
            SELECT ROUND(((point(e.lon_dec, e.lat_dec) <@> point(%s, %s)) * 1.61)::numeric, 2) distance_km,
                   e.omm_id, e.nombre
            FROM estacion e
            ORDER BY 1 ASC
            LIMIT 1
            """

            cursor = weather_db_connection.cursor()
            cursor.execute(nearest_station_query, (coord_x, coord_y))

            result = cursor.fetchone()
            logging.debug('Found station "%s" at %s kilometers from (%s, %s) for location "%s".' % (
                result[2], result[0], coord_y, coord_x, name
            ))
            loc_yaml['weather_station'] = result[1]
        else:
            try:
                loc_yaml['weather_station'] = int(loc_yaml['weather_station'])
            except:
                raise RuntimeError('Failed to parse weather_station field to int for location "%s".' % name)

            find_station = """
            SELECT e.nombre, e.lat_dec, e.lon_dec FROM estacion e WHERE e.omm_id = %s
            """

            cursor = weather_db_connection.cursor()
            cursor.execute(find_station, (loc_yaml['weather_station'], ))

            result = cursor.fetchone()
            if not result:
                raise RuntimeError('No weather station found with id = %s for location "%s".' %
                                   (loc_yaml['weather_station'], name))

            if 'name' not in loc_yaml:
                # Create a name based on the weather station that's being used.
                loc_yaml['name'] = result[0]

            if 'coord_x' not in loc_yaml or len(loc_yaml['coord_x']) == 0:
                # Set x coordinate (longitude).
                loc_yaml['coord_x'] = '%s' % result[2]

            if 'coord_y' not in loc_yaml or len(loc_yaml['coord_y']) == 0:
                # Set y coordinate (latitude).
                loc_yaml['coord_y'] = '%s' % result[1]

        if 'name' not in loc_yaml:
            loc_yaml['name'] = ''

        self.update(loc_yaml)
