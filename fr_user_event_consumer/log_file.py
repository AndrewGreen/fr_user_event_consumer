import os

class LogFile:

    def __init__(
        self,
        filename,
        directory,
        timestamp,
        impression_type = None,
        status = None,
        consumed_events = None,
        invalid_lines = None
    ):
        self.filename = filename
        self.directory = directory
        self.timestamp = timestamp
        self.impression_type = impression_type
        self.status = status
        self.consumed_events = consumed_events
        self.invalid_lines = invalid_lines

    def lines( self ):
        filename = os.path.join( self.directory, self.filename )
        with open( filename ) as stream:
            for l in stream:
                yield l
