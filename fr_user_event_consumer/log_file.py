import os

class LogFile:

    def __init__(
        self,
        filename,
        directory,
        timestamp,
        event_type,
        sample_rate = None,
        status = None,
        consumed_events = None,
        invalid_lines = None
    ):
        self.filename = filename
        self.directory = directory
        self.timestamp = timestamp
        self.event_type = event_type
        self.sample_rate = sample_rate
        self.status = status
        self.consumed_events = consumed_events
        self.invalid_lines = invalid_lines


    def lines( self ):
        filename = os.path.join( self.directory, self.filename )
        with open( filename ) as stream:
            for l in stream:
                yield l
