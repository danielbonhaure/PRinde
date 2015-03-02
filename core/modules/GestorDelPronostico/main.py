# -*- coding: utf-8 -*-
__author__ = 'Federico Schmidt'

from core.modules.PreparadorDeSimulaciones.WeatherSeriesMaker import WeatherSeriesMaker


class ForecastManager:

    def __init__(self, system_config):
        self.system_config = system_config
        self.seriesMaker = WeatherSeriesMaker(self.system_config)

    def start(self):
        # Create scheduler, etc.
        self.run_campaigns()
        pass

    def run_campaigns(self):
        for wth_station in self.system_config.omm_ids:
            print("Creating experiment data for station with OMM ID = %s." % wth_station)
            # Get soil data.
            # Crear series climáticas
            self.seriesMaker.create_series(wth_station)
            # Escribir archivos de campaña.
            # Llamar pSIMS.