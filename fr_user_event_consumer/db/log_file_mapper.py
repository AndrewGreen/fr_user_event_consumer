import mysql.connector as mariadb

from fr_user_event_consumer.log_file import LogFile, LogFileStatus
from fr_user_event_consumer import db

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
    '  filename = %(filename)s,'
    '  impressiontype = %(impressiontype)s,'
    '  timestamp = %(timestamp)s,'
    '  directory = %(directory)s,'
    '  sample_rate = %(sample_rate)s,'
    '  status = %(status)s,'
    '  consumed_events = %(consumed_events)s,'
    '  ignored_events = %(ignored_events)s,'
    '  invalid_lines = %(invalid_lines)s '
    'WHERE'
    '  id = %(db_id)s'
)

LATEST_TIME_SQL = 'SELECT timestamp FROM files ORDER BY timestamp DESC LIMIT 1'

FILES_WITH_PROCESSING_STATUS_SQL = (
    'SELECT EXISTS ( SELECT 1 FROM files WHERE status = \'processing\' LIMIT 1)' )

CACHE_KEY_PREFIX = 'LogFile'


def known( filename ):
    if db.object_in_cache( _make_cache_key( filename ) ):
        return True

    cursor = db.connection.cursor()
    cursor.execute( FILE_KNOWN_SQL, ( filename, ) )
    result = bool( cursor.fetchone()[ 0 ] )
    cursor.close()
    return result


def new(
        filename,
        directory,
        time,
        event_type,
        status = None,
        sample_rate = None,
        consumed_events = None,
        ignored_events = None,
        invalid_lines = None
    ):

    file = LogFile( filename, directory, time, event_type, sample_rate,
        status, consumed_events, ignored_events, invalid_lines )

    cursor = db.connection.cursor()

    try:
        cursor.execute( INSERT_FILE_SQL, {
            'filename': filename,
            'impressiontype': event_type.legacy_key,
            'timestamp': time,
            'directory': directory,
            'sample_rate': sample_rate,
            'status': status.value,
            'consumed_events': consumed_events,
            'ignored_events': ignored_events,
            'invalid_lines': invalid_lines
        } )

        file.db_id = cursor.lastrowid

    except mariadb.Error as e:
        db.connection.rollback()
        cursor.close()
        raise e

    db.connection.commit()
    cursor.close()

    db.set_object_in_cache( _make_cache_key( filename ), file )
    return file


def save( file ):

    # Sanity check: file should already be in the cache
    if db.get_cached_object( _make_cache_key( file.filename ) ) != file:
        raise RuntimeError(
            ( 'Attempting to save existing log file {} but it\'s not in the cache, or '
            'a different object is in the cache.' ).format( file.filename )
        )

    cursor = db.connection.cursor()

    try:
        cursor.execute( UPDATE_FILE_SQL, {
            'filename': file.filename,
            'impressiontype': file.event_type.legacy_key,
            'timestamp': file.time,
            'directory': file.directory,
            'sample_rate': file.sample_rate,
            'status': file.status.value,
            'consumed_events': file.consumed_events,
            'ignored_events': file.ignored_events,
            'invalid_lines': file.invalid_lines,
            'db_id': file.db_id
        } )

    except mariadb.Error as e:
        db.connection.rollback()
        cursor.close()
        raise e

    db.connection.commit()
    cursor.close()


def get_lastest_time():
    cursor = db.connection.cursor()
    cursor.execute( LATEST_TIME_SQL )
    row = cursor.fetchone()
    cursor.close()
    return row[0] if row else None


def files_with_processing_status():
    cursor = db.connection.cursor()
    cursor.execute( FILES_WITH_PROCESSING_STATUS_SQL )
    result = bool( cursor.fetchone()[ 0 ] )
    cursor.close()
    return result


def load_file( filename ):
    # stub
    pass


def _make_cache_key( filename ):
    return CACHE_KEY_PREFIX + filename
