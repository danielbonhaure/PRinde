import unittest

from core.lib.geo.grid import latlon_to_grid


class TestGeoLib(unittest.TestCase):

    def test_latlonToGrid(self):
        grid_coords = latlon_to_grid(lat_dec=-23.875, lon_dec=34.625, resolution=15)
        self.assertEqual(grid_coords.row, 456)
        self.assertEqual(grid_coords.column, 859)
        return