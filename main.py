#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'Federico Schmidt'

import os
import yaml

from core.modules.Configuration import Configuration
from core.modules.GestorDelPronostico.events import register_signals
from core.modules.GestorDelPronostico.boot import boot_system
from core.modules.GestorDelPronostico.main import ForecastManager
from core.lib.io.file import absdirname



# Main class definition.
class Main():

    def __init__(self):
        # Get system root path.
        self.root_path = absdirname(__file__)

        self.system_config = Configuration(self.root_path)
        self.system_config.load()

        self.system_config.root_path = self.root_path

        self.fm = ForecastManager(self.system_config)

    def bootstrap(self):
        boot_system(self.root_path, self.system_config)

        # Register a handler SIGINT/SIGTERM signals.
        register_signals()

    def run(self):
        self.bootstrap()
        self.fm.start()


# Start running the system.
Main().run()