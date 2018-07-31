from .log_file_status import LogFileStatus

class LogFile:

    def __init__(
        self,
        filename,
        directory,
        timestamp,
        impression_type,
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