import mysql.connector as mariadb

FILE_KNOWN_SQL = 'SELECT EXISTS (SELECT 1 FROM files_processed WHERE filename = %s)'

INSERT_FILE_SQL = (
    'INSERT INTO files_processed ('
    '  filename,'
    '  impressiontype,'
    '  timestamp,'
    '  directory,'
    '  status,'
    '  consumed_events,'
    '  invalid_lines'
    ') '
    'VALUES ('
    '  %(filename)s,'
    '  %(impressiontype)s,'
    '  %(timestamp)s,'
    '  %(directory)s,'
    '  %(status)s,'
    '  %(consumed_events)s,'
    '  %(invalid_lines)s'
    ')'
)

UPDATE_FILE_SQL = (
    'UPDATE files_processed SET'
    '  impressiontype = %(impressiontype)s,'
    '  timestamp = %(timestamp)s,'
    '  directory = %(directory)s,'
    '  status = %(status)s,'
    '  consumed_events = %(consumed_events)s,'
    '  invalid_lines = %(invalid_lines)s '
    'WHERE'
    '  filename = %(filename)s'
)


def file_known( file, connection ):
    cursor = connection.cursor()
    cursor.execute( FILE_KNOWN_SQL, ( file.filename, ) )
    result = bool( cursor.fetchone()[ 0 ] )
    cursor.close()
    return result


def save_file( file, connection ):
    if file.status is None:
        raise ValueError( 'File status must be set before file can be saved.' )

    cursor = connection.cursor()

    # We don't use ON DUPLICATE KEY UPDATE because that increases id on update.
    # So, check if the file is known, and if so, UPDATE, otherwise, INSERT.
    cursor.execute( FILE_KNOWN_SQL, ( file.filename, ) )
    sql_command = UPDATE_FILE_SQL if bool( cursor.fetchone()[ 0 ] ) else INSERT_FILE_SQL

    try:
        cursor.execute( sql_command, {
            'filename': file.filename,
            'impressiontype': file.event_type.legacy_key,
            'timestamp': file.timestamp,
            'directory': file.directory,
            'status': file.status.value,
            'consumed_events': file.consumed_events,
            'invalid_lines': file.invalid_lines
        } )

    except mariadb.Error as e:
        connection.rollback()
        cursor.close()
        raise e

    connection.commit()
    cursor.close()
