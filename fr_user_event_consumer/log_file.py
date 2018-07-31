from .log_file_statuses import LogFileStatus

class LogFile:

    def __init__(
        self,
        filename,
        directory,
        timestamp,
        status = LogFileStatus.NEW,
        consumed_events = None,
        invalid_lines = None
    ):
        self.filename = filename
        self.directory = directory
        self.timestamp = timestamp
        self.status = status
        self.consumed_events = consumed_events
        self.invalid_lines = invalid_lines