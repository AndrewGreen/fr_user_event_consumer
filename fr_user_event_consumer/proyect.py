import re

validation_pattern = re.compile( '^[a-z0-9\-_\.]+$' )

class Project:

    def __init__( self, identifier ):
        if validation_pattern.match( identifier ):
            self.identifier = identifier
            self.valid = True
        else:
            self.identifier = None
            self.valid = False