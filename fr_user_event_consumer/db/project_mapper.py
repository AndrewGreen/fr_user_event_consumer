import mysql.connector as mariadb

from fr_user_event_consumer.proyect import Project
from fr_user_event_consumer import db

GET_PROJECT_SQL = 'SELECT id FROM project WHERE project = %s'

INSERT_PROJECT_SQL = (
    'INSERT INTO project ('
    '  project'
    ') '
    'VALUES ('
    '  %(identifier)s'
    ')'
)

CACHE_KEY_PREFIX = 'Project'


def get_or_new( identifier ):
    cache_key = CACHE_KEY_PREFIX + identifier

    project = db.get_cached_object( cache_key )
    if project:
        return project

    cursor = db.connection.cursor()
    cursor.execute( GET_PROJECT_SQL, ( identifier, ) )
    row = cursor.fetchone()

    # This will raise an error if the identifier is not valid
    # It's important to do this before inserting into the database.
    project = Project( identifier )

    if row is not None:
        project.db_id = row[ 0 ]
    else:
        try:
            cursor.execute( INSERT_PROJECT_SQL, {
                'identifier': identifier
            } )

            project.db_id = cursor.lastrowid

        except mariadb.Error as e:
            db.connection.rollback()
            cursor.close()
            raise e

        db.connection.commit()

    cursor.close()
    db.set_object_in_cache( cache_key, project )
    return project
