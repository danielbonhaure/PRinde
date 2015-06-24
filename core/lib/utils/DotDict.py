from collections import MutableMapping
import json

__author__ = 'Federico Schmidt'


class DotDict(MutableMapping):

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
        return json.dumps(self, default=lambda o: o.__dict__)
