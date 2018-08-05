import re

validation_pattern = re.compile( '^[a-zA-Z]{2,2}$' )

class Country:

    def __init__( self, country_code ):
        if validation_pattern.match( country_code):
            self.country_code = country_code
            self.valid = True
        else:
            self.country_code = None
            self.valid = False