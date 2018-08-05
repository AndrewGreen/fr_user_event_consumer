import mysql.connector as mariadb

FILE_KNOWN_SQL = 'SELECT EXISTS (SELECT 1 FROM files_processed WHERE filename = %s)'

SAVE_FILE_SQL = (
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
    ') '
    'ON DUPLICATE KEY UPDATE '
    '  impressiontype = %(impressiontype)s,'
    '  timestamp = %(timestamp)s,'
    '  directory = %(directory)s,'
    '  status = %(status)s,'
    '  consumed_events = %(consumed_events)s,'
    '  invalid_lines = %(invalid_lines)s'
)


# This should be set by the caller before other methods are called
connection = None


def file_known( file ):
    cursor = connection.cursor()
    cursor.execute( FILE_KNOWN_SQL, ( file.filename, ) )
    result = bool( cursor.fetchone()[ 0 ] )
    cursor.close()
    return result


def save_file( file ):
    cursor = connection.cursor()

    if file.status is None:
        raise ValueError( 'File status must be set before file can be saved.' )

    try:
        cursor.execute( SAVE_FILE_SQL, {
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
