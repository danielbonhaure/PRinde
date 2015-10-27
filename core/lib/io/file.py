# coding=utf-8
import os

__author__ = 'Federico Schmidt'


def absdirname(f):
    """
    Devuelve el path absoluto a la carpeta que contiene al archivo.
    @param f
    """
    return os.path.abspath(os.path.dirname(os.path.realpath(f)))


def listdir_fullpath(d, onlyFiles=False, recursive=False, filter=None):
    ls = os.listdir(d)
    if len(ls) == 0:
        return []
    full_path = [os.path.join(d, f) for f in ls]

    if recursive:
        for d in full_path:
            if os.path.isdir(d):
                full_path = full_path + listdir_fullpath(d, onlyFiles=onlyFiles, recursive=True)

    if onlyFiles:
        full_path = [f for f in full_path if os.path.isfile(f)]
    if filter:
        full_path = [f for f in full_path if filter(f)]
    return full_path


def clean_folder(folder, onlyfiles=False):
    """
    Vacía el contenido de una carpeta de ser posible.
    @param folder
    @param onlyfiles: Especifica si se deben borrar únicamente archivo o también carpetas (default = archivos y carpetas).
    @return: True en caso de éxito o False de lo contrario.
    """
    for f in os.listdir(folder):
        file_path = os.path.join(folder, f)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif not onlyfiles:
                if len(os.listdir(file_path)) > 0:
                    clean_folder(file_path)
                else:
                    os.rmdir(file_path)
        except Exception, e:
            print e
            return False
    return True


def create_folder(folder):
    """
    Crea o vacía la carpeta folder de ser posible.
    Devuelve True en caso de éxito o False en caso de haber un error.

    @param folder: la carpeta a crear o eliminar.
    @return True en caso de éxito o False de lo contrario.
    """
    # Verificamos si la carpeta de salida ya existe.
    if os.path.exists(folder):
        if os.path.isfile(folder):
            # Si es un archivo, intentamos borrarlo y crear el directorio.
            try:
                os.remove(folder)
                os.mkdir(folder)
            except IOError:
                print "No se pudo remover el archivo para crear la carpeta:" + folder + "."
                return False
    else:
        # Si no existe, la intentamos crear.
        try:
            os.makedirs(folder)
        except Exception, ex:
            print "No se pudo crear la carpeta: " + folder + ". Exception: %s" % ex
    return os.path.exists(folder) and not os.path.isfile(folder)


def fileNameWithoutExtension(file):
    return os.path.splitext(os.path.basename(file))[0]


def create_folder_with_permissions(parent, folder_name=None, permissions=0777):
    if not folder_name:
        folder_name = os.path.basename(parent)
        parent = os.path.dirname(parent)

    if not os.path.isdir(parent):
        return False

    folder = os.path.join(parent, folder_name)

    if not create_folder(folder):
        raise RuntimeError('Failed to create folder "%s".' % folder)
    else:
        # Le cambiamos los permisos a la carpeta.
        os.chmod(folder, permissions)
        return os.path.abspath(folder)