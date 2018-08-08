import re
from datetime import timedelta
import logging
import mysql.connector as mariadb

from fr_user_event_consumer.central_notice_event import CentralNoticeEvent
from fr_user_event_consumer.db import project_mapper, language_mapper
from fr_user_event_consumer import db

INSERT_DATA_CELL_SQL = (
    'INSERT INTO bannerimpressions ('
    '  timestamp,'
    '  banner,'
    '  campaign,'
    '  project_id,'
    '  language_id,'
    '  country_id,'
    '  count,'
    '  file_id'
    ') '
    'VALUES ('
    '  %(timestamp)s,'
    '  %(banner)s,'
    '  %(campaign)s,'
    '  %(project_id)s,'
    '  %(language_id)s,'
    '  %(country_id)s,'
    '  %(count)s,'
    '  %(file_id)s'
    ')'
)

# Strings for languages and projects not separated out, from legacy
_OTHER_PROJECT_IDENTIFIER = 'other_project'
_OTHER_LANGUAGE_CODE = 'other'

_other_project = None
_other_language = None
_detail_languages = None
_detail_projects_pattern = None
_sample_rate_multiplier = None
_file = None
_data = None

logger = logging.getLogger( __name__ )


def new_unsaved( json_string ):
    return CentralNoticeEvent( json_string )


def begin_aggregation( detail_languages, detail_projects_regex, sample_rate, file ):
    global _detail_languages, _detail_projects_pattern, _sample_rate_multiplier
    global _data, _file

    _detail_languages = detail_languages
    _detail_projects_pattern = re.compile( detail_projects_regex )
    _sample_rate_multiplier = 100 / sample_rate
    _file = file
    _data = {}


def aggregate( event ):
    global data

    # Grouping less common projects as languages
    if _detail_projects_pattern.match( event.project.identifier ):
        project = event.project
    else:
        project = _get_other_project()

    if event.language.language_code in _detail_languages:
        language = event.project
    else:
        language = _get_other_language()

    # Remove seconds and microseconds from time to group by minute
    time = event.time - timedelta( seconds = event.time.second,
        microseconds = event.time.microsecond )

    banner = event.banner
    campaign = event.campaign
    country = event.country

    cell_id = _data_cell_id( time, banner, campaign, project, language, country )

    cell = _data.get( cell_id )
    if not cell:
        cell = CNDataCell( time, banner, campaign, project, language, country )
        _data[ cell_id ] = cell

    cell.event_count += _sample_rate_multiplier


def end_aggregation():
    logger.debug( f'Aggregating {len(_data)} cells' )

    cursor = db.connection.cursor()

    for cell in _data.values():

        try:
            cursor.execute( INSERT_DATA_CELL_SQL, {
                'timestamp': cell.time,
                'banner': cell.banner,
                'campaign': cell.campaign,
                'project_id': cell.project.db_id,
                'language_id': cell.language.db_id,
                'country_id': cell.country.db_id,
                'count': cell.event_count,
                'file_id': _file.db_id
            } )

        except mariadb.Error as e:
            db.connection.rollback()
            cursor.close()
            raise e

    db.connection.commit()
    cursor.close()


def _get_other_project():
    global _other_project
    if _other_project is None:
        _other_project = project_mapper.get_or_new( _OTHER_PROJECT_IDENTIFIER )
    return _other_project


def _get_other_language():
    global _other_language
    if _other_language is None:
        _other_language = language_mapper.get_or_new( _OTHER_LANGUAGE_CODE )
    return _other_language


def _data_cell_id( time, banner, campaign, project, language, country ):
    return (
        time.strftime( '%Y%m%d%H%M%S' ) +
        banner +
        campaign +
        project.identifier +
        language.language_code +
        country.country_code
    )


class CNDataCell:
    def __init__( self, time, banner, campaign, project, language, country ):
        self.time = time
        self.banner = banner
        self.campaign = campaign
        self.project = project
        self.language = language
        self.country = country

        self.event_count = 0
