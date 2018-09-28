import os
import psycopg2
import pymongo

__author__ = 'Federico Schmidt'


class DatabaseUtils:

    def __init__(self):
        pass

    @staticmethod
    def connect_postgresql(conn_dictionary, config_path='.'):
        conn_dictionary = DatabaseUtils.__validate_connection_dict__(conn_dictionary)
        conn_name = conn_dictionary['name']

        if 'user' not in conn_dictionary:
            raise RuntimeError('Missing user name for database connection "%s".' % conn_name)

        if 'port' not in conn_dictionary:
            conn_dictionary['port'] = 5432

        if 'password' not in conn_dictionary:
            passwd = DatabaseUtils.__password_lookup__(conn_dictionary['user'], config_path)
            if not passwd:
                raise RuntimeError('Missing password or password file for database connection "%s".' % conn_name)
            conn_dictionary['password'] = passwd

        try:
            conn = psycopg2.connect(host=conn_dictionary['host'],
                                    user=conn_dictionary['user'],
                                    database=conn_dictionary['db_name'],
                                    port=conn_dictionary['port'],
                                    password=conn_dictionary['password'])

            return conn
        except Exception as e:
            raise RuntimeError('Failed to create database connection "%s". Reason: "%s".' % (conn_name, str(e).strip()))

    @staticmethod
    def connect_mongodb(conn_dictionary, config_path='.'):
        conn_dictionary = DatabaseUtils.__validate_connection_dict__(conn_dictionary)
        conn_name = conn_dictionary['name']

        if 'port' not in conn_dictionary:
            conn_dictionary['port'] = 27017

        uri = 'mongodb://'

        if 'user' in conn_dictionary:
            uri += conn_dictionary['user'] + ':'

            if 'password' not in conn_dictionary:
                passwd = DatabaseUtils.__password_lookup__(conn_dictionary['user'], config_path)
                if not passwd:
                    raise RuntimeError('Missing password or password file for database connection "%s".' % conn_name)
            else:
                passwd = conn_dictionary['password']

            uri += passwd + '@'

        uri += conn_dictionary['host'] + ':' + str(conn_dictionary['port'])

        try:
            client = pymongo.MongoClient(uri)
            db_name = conn_dictionary['db_name']

            if db_name not in client.database_names():
                raise RuntimeError('No such database (%s) at mongo connection "%s".' % (db_name, uri))

            return client[db_name]
        except Exception as e:
            raise RuntimeError('Failed to create database connection "%s". Reason: "%s".' % (conn_name, str(e).strip()))

    @staticmethod
    def __validate_connection_dict__(conn_dictionary):
        if 'name' not in conn_dictionary:
            conn_dictionary['name'] = repr(conn_dictionary)

        if 'db_name' not in conn_dictionary:
            raise RuntimeError('Missing database name for database connection "%s".' % conn_dictionary['name'])

        if 'host' not in conn_dictionary:
            conn_dictionary['host'] = 'localhost'

        return conn_dictionary

    @staticmethod
    def __password_lookup__(username, config_path='.'):
        pwd_file_path = os.path.join(config_path, 'pwd', username+'.pwd')

        if not os.path.isfile(pwd_file_path) or not os.path.exists(pwd_file_path):
            return None
        with open(pwd_file_path, mode='r') as f:
            return f.read()
