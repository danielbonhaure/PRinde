import json
import logging
from lib.jobs.base import BaseJob
from pymongo import IndexModel, ASCENDING
import sys
__author__ = 'Federico Schmidt'


class CheckWeatherDB(BaseJob):
    def __init__(self, system_config):
        super(CheckWeatherDB, self).__init__(name='Check PostgreSQL weather DB structure.')
        self.system_config = system_config
        self.needed_tables = {'estacion_registro_diario', 'estacion', 'estacion_registro_diario_imputado',
                              'estacion_radiacion_diaria'}
        self.needed_materialized_views = {'estacion_registro_diario_completo'}

    def run(self):
        if 'weather_db' not in self.system_config.database:
            raise RuntimeError('No weather database connection was provided. '
                               'Plase provide one under the "weather_db" key.')

        wth_db_connection = self.system_config.database['weather_db']
        cursor = wth_db_connection.cursor()

        cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        tables_set = {r[0] for r in cursor}

        if self.needed_tables & tables_set != self.needed_tables:
            table_diff = list(self.needed_tables - tables_set)
            raise RuntimeError('Weather database schema has missing tables: %s. '
                               'Consider running setup.sh again.' % table_diff)

        cursor.execute("SELECT relname FROM pg_class WHERE relkind = 'm'")
        materialized_views_set = {r[0] for r in cursor}

        if self.needed_materialized_views & materialized_views_set != self.needed_materialized_views:
            views_diff = list(self.needed_materialized_views - materialized_views_set)
            raise RuntimeError('Weather database schema has missing materialized views: %s. '
                               'Consider running setup.sh again.' % views_diff)


class CheckRindeDB(BaseJob):
    def __init__(self, system_config):
        super(CheckRindeDB, self).__init__(name='Check Mongo output DB')
        self.needed_collections = {'locations', 'forecasts', 'reference_rainfall', 'reference_simulations',
                                   'simulations'}
        self.collection_indexes = {
            'locations': {'_id'},
            'reference_simulations': {'water_content', 'location_id', '_id', 'soil_id', 'crop_type'},
            'reference_rainfall': {'omm_id', '_id'},
            'forecasts': {'_id', 'forecast_date'},
            'simulations': {'forecast_date', 'location_id', '_id', 'crop_type'}
        }
        self.system_config = system_config

    def run(self):
        db = self.system_config.database['rinde_db']
        collections = set(db.collection_names())

        if self.needed_collections & collections != self.needed_collections:
            coll_diff = self.needed_collections - collections

            for c in coll_diff:
                db.create_collection(c)

            logging.getLogger().info('Created collections %s in output DB.' % coll_diff)

        # Check and create indexes.
        for collection in self.needed_collections:
            collection = db[collection]
            collection_indexed_fields = set()
            expected_indexes = self.collection_indexes[collection.name]

            for idx in collection.index_information().values():
                # Append fields to the set.
                collection_indexed_fields |= {field[0] for field in idx['key']}

            if expected_indexes & collection_indexed_fields != expected_indexes:
                idx_diff = expected_indexes - collection_indexed_fields

                for index_field in idx_diff:
                    # index = IndexModel(keys=[(index_field, ASCENDING)], )
                    collection.create_index(index_field, name=index_field, background=True, sparse=True)

                logging.info('Created indexes %s in collection %s.' % (idx_diff, collection.name))