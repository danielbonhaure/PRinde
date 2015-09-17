# -*- coding: utf-8 -*-
from collections import MutableMapping
import json

__author__ = 'Federico Schmidt'


def group_by(collection, function):
    """
    Agrupa una lista en un mapa donde la clave es la función pasada como parámetro.
    """
    d = dict()
    if isinstance(collection, dict):
        collection = collection.iteritems()
    for f in collection:
        key = function(f)

        if key in d:
            d[key] += [f]
        else:
            d[key] = [f]
    return d


class DotDict(MutableMapping):
    """
    A dictionary that allows accessing every key with dot notation, like a class property.
    """

    def __init__(self, original_dict=None):
        if not original_dict:
            original_dict = dict()
        else:
            # Convert nested dictionaries.
            for key, value in original_dict.iteritems():
                if isinstance(value, dict):
                    original_dict[key] = DotDict(original_dict[key])

        self.__dict__.update(original_dict)

    def __getitem__(self, key):
        return self.__dict__[key]

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __delitem__(self, key):
        del self.__dict__[key]

    def __iter__(self):
        return iter(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

    def __repr__(self):
        return self.__dict__.__repr__()

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__ if hasattr(o, '__dict__') else str(o))
