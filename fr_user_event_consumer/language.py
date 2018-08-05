import re

validation_pattern = re.compile( '^[a-z\-_]+$' )

class Language:

    def __init__( self, language_code ):
        if validation_pattern.match( language_code):
            self.language_code = language_code
            self.valid = True
        else:
            self.language_code = None
            self.valid = False