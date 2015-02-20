#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'Federico Schmidt'

import os
import yaml
import signal
import core.events
from core.boot import boot_system

from core.io.file import absdirname

# Get system root path.
root_path = absdirname(__file__)

# Create system configuration path relative to root.
config_path = os.path.join(root_path, 'config', 'system.yaml')

if not os.path.isfile(config_path):
    raise RuntimeError('System configuration file not found.')


system_config = yaml.safe_load(open(config_path))

boot_system(root_path, system_config)

print(system_config.get('temp_folder'))

# Register a handler SIGINT/SIGTERM signals.
core.events.register_signals()

