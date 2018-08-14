import re
import json
import logging
from datetime import datetime

from fr_user_event_consumer import country, project, language

EVENT_TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%SZ' # Coordinate with EventLogging
validate_banner_pattern = re.compile( '^[A-Za-z0-9_]+$' ) # Coordinate with CentralNotice

_logger = logging.getLogger( __name__ )

class CentralNoticeEvent:

    def __init__( self, json_string ):
        self._raw_json = json_string

        # Validate event data
        self.valid = False

        try:
            self._data = json.loads( json_string )
        except ValueError as e:
            _logger.debug( 'Invalid Json: {}'.format( e ) )
            return

        self.bot = self._data[ 'userAgent' ][ 'is_bot' ]
        self.testing = self._data[ 'event' ].get( 'testingBanner', False )
        self.banner_shown = self._data[ 'event' ][ 'statusCode' ] == '6' # Received as string

        self.country_code = self._data[ 'event' ][ 'country' ]
        if not country.is_valid_country_code( self.country_code ):
            return

        self.language_code = self._data[ 'event' ][ 'uselang' ]
        if not language.is_valid_language_code( self.language_code ):
            return

        self.project_identifier = self._data[ 'event' ][ 'db' ]
        if not project.is_valid_identifier( self.project_identifier ):
            return

        if 'banner' in self._data[ 'event' ]:
            self.banner = self._data[ 'event' ][ 'banner' ]
            if not validate_banner_pattern.match( self.banner ):
                _logger.debug( 'Invalid banner: {}'.format( self.banner ) )
                return
        else:
            self.banner = None

        # TODO Add campaign name validation when that's included in CentralNotice
        self.campaign = self._data[ 'event' ][ 'campaign' ]

        # Something is wrong if this isn't a banner preview (testing) but there's no
        # campaign
        if ( not self.campaign ) and ( not self.testing ):
            return

        try:
            self.time = datetime.strptime( self._data[ 'dt' ], EVENT_TIMESTAMP_FORMAT )
        except ValueError as e:
            _logger.debug( 'Invalid timestamp: {}'.format( e ) )
            return

        self.valid = True
