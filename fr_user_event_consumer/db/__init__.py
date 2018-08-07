import mysql.connector as mariadb

from . import ( log_file_mapper, central_notice_event_mapper, country_mapper,
     language_mapper, project_mapper )

connection = None
object_cache = {}

def connect( user, password, host, database ):
    global connection

    if connection is not None:
        connection.close()
        raise RuntimeError( 'Attempt to connect to DB after connection already created.' )

    # Do we need to ensure this closes on error?
    connection = mariadb.connect( user = user, password = password, host = host,
        database = database )

    return connection


def close():
    global connection

    if connection is None:
        raise RuntimeError( 'Attempt to close DB before connection was created.' )

    connection.close()
    connection = None


def get_cached_object( key ):
    return object_cache.get( key, None )


def set_object_in_cache( key, obj ):
    object_cache[ key ] = obj


def object_in_cache( key ):
    return key in object_cache