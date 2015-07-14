# -*- coding: utf-8 -*-
import subprocess
import core.lib.utils.collection_utils as collection_utils

__author__ = 'Federico Schmidt'

import os, glob, getpass

def read_connection_parameters():
    conn = collection_utils.namedtuple('Connection', ['db_name', 'user'])

    conn.db_name = raw_input("Ingrese el nombre de la Base de Datos [crcssa]: ")
    if not conn.db_name:
        conn.db_name = "crcssa"

    conn.user = raw_input("Ingrese el usuario de la Base de Datos [postgres.pwd]: ")
    if not conn.user:
        conn.user = "postgres.pwd"

    pwd = getpass.getpass("Ingrese la contraseña para el usuario '%s': " % conn.user)
    if not pwd:
        print 'La contraseña no puede ser vacía.'
        exit(1)

    # Escribimos en la variable de entorno la contraseña de postgres.pwd para evitar pasarla en plaintext al proceso.
    os.environ["PGPASSWORD"] = pwd

    return conn


def run_script(postgresql, user, db_name, script_path, port=5432, script_parameter=None):
    # Armamos el comando que le vamos a mandar a la consola.
    cmd = "\"%s\" -p %s -U %d -h localhost " % (postgresql, port, user)
    # Agregamos al comando el parámetro que indica la carpeta de salida de la query SQL (si fue definido).
    if script_parameter:
        cmd += "-v v1=\"'%s'\" " % script_parameter
    # Agregamos la DB donde se debe ejecutar la consulta.
    cmd += "-d %s " % db_name
    # Agregamos la ubicación del archivo con la query SQL a ejecutar.
    cmd += "-f \"%s\"" % script_path

    try:
        # Intentamos ejecutar el comando.
        output = subprocess.check_output(cmd, shell=True, env=os.environ)
        print " [OK]"
    except:
        # De ocurrir una excepción quiere decir que no se pudo ejecutar correctamente el comando.
        print "No se pudo ejecutar el comando correctamente."
        raise RuntimeError("Couldn't execute the command: \"%s\"" % cmd)


def find():
    if(os.name == 'nt'):
        print "Buscando instalación de PostgreSQL..."
        program_files = "C:/Program Files"
        program_files_x86 = "C:/Program Files (x86)"

        search_prg_files = search_filename("psql.exe", program_files)
        search_prg_files_x86 = search_filename("psql.exe", program_files_x86)

        search = search_prg_files + search_prg_files_x86

        if len(search) == 1:
            return os.path.abspath(search[0])
        elif len(search) > 1:
            print "¿Qué instalación le gustaría usar?:"
            for index, row in enumerate(search):
                print '\t * %s: %s' % (index+1, os.path.normpath(row))

            index = raw_input('Ingrese el índice de la instalación [1]: ')
            if not index:
                index = 1
            else:
                index = int(index)

            if index > len(search) or index < 1:
                print 'El índice no ingresado no es válido.'
                exit(1)
            else:
                return os.path.abspath(search[index - 1])
    else:
        return "psql"


def search_filename(file_name, path, depth="*/*/*/*"):
    search = glob.glob(os.path.join(path, depth))
    return filter(lambda f: (os.path.isfile(f) and os.path.basename(f) == file_name), search)
