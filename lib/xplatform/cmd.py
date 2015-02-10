__author__ = 'Federico Schmidt'

import os
import sys

def cleanTerminal():
    os.system('cls' if (os.name == 'nt') else 'clear')

def print_same_line(text):
    sys.stdout.write(text)
    sys.stdout.flush()