import re

validate_regex = re.compile( '^[a-z0-9\-_\.]+$' )

class Project:

    def __init__( self, project_identifier ):
        if validate_regex.match( project_identifier):
            self.project_identifier = project_identifier
            self.valid = True
        else:
            self.project_identifier = None
            self.valid = False