# -*- coding: utf-8 -*-
__author__ = 'Federico Schmidt'

def group_by(list, function):
    """
    Agrupa una lista en un mapa donde la clave es la función pasada como parámetro.
    """
    d = dict()
    for f in list:
        key = function(f)

        if key in d:
            d[key] += [f]
        else:
            d[key] = [f]
    return d