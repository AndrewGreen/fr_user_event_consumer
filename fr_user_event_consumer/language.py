import re

validation_pattern = re.compile( '^[a-z\-_]+$' )

class Language:

    def __init__( self, language_code ):
        if not validation_pattern.match( language_code):
            raise ValueError( 'Invalid language code: {}'.format( language_code ) )

        self.language_code = language_code
        self.db_id = None
