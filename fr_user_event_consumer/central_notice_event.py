import re
import json
import logging
from datetime import datetime

EVENT_TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%SZ' # Coordinate with EventLogging
validate_banner_pattern = re.compile( '^[A-Za-z0-9_]+$' ) # Coordinate with CentralNotice

from fr_user_event_consumer.country import Country
from fr_user_event_consumer.language import Language
from fr_user_event_consumer.proyect import Project

logger = logging.getLogger( __name__ )

class CentralNoticeEvent:

    def __init__( self, json_string ):
        self._raw_json = json_string

        # Validate event data
        self.valid = False

        try:
            self._data = json.loads( json_string )
        except ValueError as e:
            logger.debug( f'Invalid Json: {e}')
            return

        country_code = self._data[ 'event' ][ 'country' ]
        self.country = Country( country_code )
        if not self.country.valid:
            logger.debug( f'Invalid country: {country_code}' )
            return

        language_code = self._data[ 'event' ][ 'uselang' ]
        self.language = Language( language_code )
        if not self.language.valid:
            logger.debug( f'Invalid country: {language_code}' )
            return

        project_identifier = self._data[ 'event' ][ 'project' ]
        self.project = Project( project_identifier )
        if not self.project.valid:
            logger.debug( f'Invalid country: {project_identifier}' )
            return

        if 'banner' in self._data[ 'event' ]:
            self._banner = self._data[ 'event' ][ 'banner' ]
            if not validate_banner_pattern.match( self._banner ):
                logger.debug( f'Invalid banner: {self._banner}' )
                return
        else:
            self._banner = None

        try:
            self.time = datetime.strptime( self._data[ 'dt' ], EVENT_TIMESTAMP_FORMAT )
        except ValueError as e:
            logger.debug( f'Invalid timestamp: {e}' )
            return

        # TODO Add campaign name validation when that's included in CentralNotice

        self.valid = True

        self.is_bot = self._data[ 'userAgent' ][ 'is_bot' ]
