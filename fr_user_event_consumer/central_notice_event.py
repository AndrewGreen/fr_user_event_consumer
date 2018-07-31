import json
import logging

logger = logging.getLogger( __name__ )

class CentralNoticeEvent:

    def __init__( self, json_string ):
        self._raw_json = json_string

        try:
            self._data = json.loads( json_string )
            self.valid = True
        except ValueError as e:
            self.valid = False
            logger.debug( f'Invalid Json: {e}')
