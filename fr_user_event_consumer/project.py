import re

validation_pattern = re.compile( '^[a-z0-9\-_\.]+$' )

def is_valid_identifier( identifier ):
    return bool( validation_pattern.match( identifier ) )


class Project:

    def __init__( self, identifier ):
        if not is_valid_identifier( identifier ):
            raise ValueError( 'Invalid project identifier {}'.format( identifier ) )

        self.identifier = identifier
        self.db_id = None
