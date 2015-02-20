#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'Federico Schmidt'

import csv
from netCDF4 import Dataset as nc


# Abrimos el archivo CSV para lectura, especificando que el delimitador es el TAB.
reader = csv.reader(open(csv_file), delimiter=delimiter)  # Escribimos el inicio de la tabla.
table_string = '<table>'

# Enumeramos cada fila del archivo CSV y las recorremos.
for index, row in enumerate(reader):
    pass
