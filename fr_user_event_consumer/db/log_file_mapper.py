import mysql.connector as mariadb

from fr_user_event_consumer.log_file import LogFile, LogFileStatus
import fr_user_event_consumer.db as db

FILE_KNOWN_SQL = 'SELECT EXISTS (SELECT 1 FROM files WHERE filename = %s)'

INSERT_FILE_SQL = (
    'INSERT INTO files ('
    '  filename,'
    '  impressiontype,'
    '  timestamp,'
    '  directory,'
    '  sample_rate,'
    '  status,'
    '  consumed_events,'
    '  ignored_events,'
    '  invalid_lines'
    ') '
    'VALUES ('
    '  %(filename)s,'
    '  %(impressiontype)s,'
    '  %(timestamp)s,'
    '  %(directory)s,'
    '  %(sample_rate)s,'
    '  %(status)s,'
    '  %(consumed_events)s,'
    '  %(ignored_events)s,'
    '  %(invalid_lines)s'
    ')'
)

UPDATE_FILE_SQL = (
    'UPDATE files SET'
    '  impressiontype = %(impressiontype)s,'
    '  timestamp = %(timestamp)s,'
    '  directory = %(directory)s,'
    '  sample_rate = %(sample_rate)s,'
    '  status = %(status)s,'
    '  consumed_events = %(consumed_events)s,'
    '  ignored_events = %(ignored_events)s,'
    '  invalid_lines = %(invalid_lines)s '
    'WHERE'
    '  filename = %(filename)s'
)

files = {}


def file_known( file ):
    cursor = db.connection.cursor()
    cursor.execute( FILE_KNOWN_SQL, ( file.filename, ) )
    result = bool( cursor.fetchone()[ 0 ] )
    cursor.close()
    return result


def new_file(
        filename,
        directory,
        timestamp,
        event_type,
        sample_rate = None,
        status = LogFileStatus.NEW,
        consumed_events = None,
        ignored_events = None,
        invalid_lines = None
    ):

    return LogFile( filename, directory, timestamp, event_type, sample_rate, status,
                    consumed_events, ignored_events, invalid_lines )


def save_file( file ):
    if file.status is None:
        raise ValueError( 'File status must be set before file can be saved.' )

    cursor = db.connection.cursor()

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
            'sample_rate': file.sample_rate,
            'status': file.status.value,
            'consumed_events': file.consumed_events,
            'ignored_events': file.ignored_events,
            'invalid_lines': file.invalid_lines
        } )

    except mariadb.Error as e:
        db.connection.rollback()
        cursor.close()
        raise e

    db.connection.commit()
    cursor.close()
