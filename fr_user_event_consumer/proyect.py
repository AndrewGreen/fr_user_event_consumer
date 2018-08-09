import re

validation_pattern = re.compile( '^[a-z0-9\-_\.]+$' )

class Project:

    def __init__( self, identifier ):
        if not validation_pattern.match( identifier ):
            raise ValueError( 'Invalid project identifier {}'.format( identifier ) )

        self.identifier = identifier
        self.db_id = None
