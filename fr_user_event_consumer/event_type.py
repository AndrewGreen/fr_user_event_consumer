from enum import Enum

class EventType(Enum):
    CENTRAL_NOTICE = ( 'banner' )
    LANDING_PAGE = ( 'landingpage' )

    def __init__( self, legacy_key ):
        self.legacy_key = legacy_key