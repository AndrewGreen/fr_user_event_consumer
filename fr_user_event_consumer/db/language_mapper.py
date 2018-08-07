import mysql.connector as mariadb

from fr_user_event_consumer.language import Language
from fr_user_event_consumer import db

GET_LANGUAGE_SQL = 'SELECT id FROM language WHERE iso_code = %s'

INSERT_LANGUAGE_SQL = (
    'INSERT INTO language ('
    '  iso_code'
    ') '
    'VALUES ('
    '  %(language_code)s'
    ')'
)

CACHE_KEY_PREFIX = 'Language'


def get_or_new( language_code ):
    cache_key = CACHE_KEY_PREFIX + language_code

    language = db.get_cached_object( cache_key )
    if language:
        return language

    cursor = db.connection.cursor()
    cursor.execute( GET_LANGUAGE_SQL, ( language_code, ) )
    row = cursor.fetchone()

    # This will raise an error if the identifier is not valid
    # It's important to do this before inserting into the database.
    language = Language( language_code )

    if row is not None:
        language.db_id = row[ 0 ]
    else:
        try:
            cursor.execute( INSERT_LANGUAGE_SQL, {
                'language_code': language_code
            } )

            language.db_id = cursor.lastrowid

        except mariadb.Error as e:
            db.connection.rollback()
            cursor.close()
            raise e

        db.connection.commit()

    cursor.close()
    db.set_object_in_cache( cache_key, language )
    return language
