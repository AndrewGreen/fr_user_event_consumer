import mysql.connector as mariadb

connection = None

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
