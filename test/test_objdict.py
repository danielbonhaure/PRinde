from core.lib.utils.extended_collections import DotDict

__author__ = 'Federico Schmidt'

import unittest


class TestGeoLib(unittest.TestCase):

    def test_initFromDict(self):
        d = { "prop1": 1, "prop2": 2}
        d = DotDict(d)

        self.assertEqual(d.prop1, d.get('prop1'))
        self.assertEqual(d.prop1, d['prop1'])

    def test_updateDict(self):
        d = DotDict()

        d.prop1 = 1
        d['prop2'] = 2

        self.assertEqual(d.prop1, d['prop1'])
        self.assertEqual(d['prop2'], 2)
        self.assertEqual(d.get('prop2'), d.prop2)

    def test_nestedDict(self):
        d = {"prop1": {
            "prop2": {
                "prop3": 3
            }
        }}
        d = DotDict(d)

        self.assertEqual(d.prop1.prop2.prop3, 3)

    def test_joinDicts(self):
        d1 = DotDict({"prop1": 1})
        d2 = DotDict({"prop2": 2})

        d1.update(d2)
        self.assertEqual(d1.prop2, 2)
