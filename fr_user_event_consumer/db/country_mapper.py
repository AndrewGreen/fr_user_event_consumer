import mysql.connector as mariadb

from fr_user_event_consumer.country import Country
from fr_user_event_consumer import db

GET_COUNTRY_SQL = 'SELECT id FROM country WHERE iso_code = %s'

INSERT_COUNTRY_SQL = (
    'INSERT INTO country ('
    '  iso_code'
    ') '
    'VALUES ('
    '  %(country_code)s'
    ')'
)

CACHE_KEY_PREFIX = 'Country'


def get_or_new( country_code ):
    cache_key = CACHE_KEY_PREFIX + country_code

    country = db.get_cached_object( cache_key )
    if country:
        return country

    cursor = db.connection.cursor()
    cursor.execute( GET_COUNTRY_SQL, ( country_code, ) )
    row = cursor.fetchone()

    # This will raise an error if the identifier is not valid
    # It's important to do this before inserting into the database.
    country = Country( country_code )

    if row is not None:
        country.db_id = row[ 0 ]
    else:
        try:
            cursor.execute( INSERT_COUNTRY_SQL, {
                'country_code': country_code
            } )

            country.db_id = cursor.lastrowid

        except mariadb.Error as e:
            db.connection.rollback()
            cursor.close()
            raise e

        db.connection.commit()

    cursor.close()
    db.set_object_in_cache( cache_key, country )
    return country
