from enum import Enum

class LogFileStatus(Enum):
    NEW = 'new'
    PROCESSING = 'processing'
    CONSUMED = 'consumed'


class LogFile:

    def __init__(
        self,
        filename,
        directory,
        timestamp,
        event_type,
        sample_rate = None,
        status = LogFileStatus.NEW,
        consumed_events = None,
        ignored_events = None,
        invalid_lines = None
    ):
        self.filename = filename
        self.directory = directory
        self.timestamp = timestamp
        self.event_type = event_type
        self.sample_rate = sample_rate
        self.status = status
        self.consumed_events = consumed_events
        self.ignored_events = ignored_events
        self.invalid_lines = invalid_lines
