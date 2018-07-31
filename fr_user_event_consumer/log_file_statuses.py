from enum import Enum

class LogFileStatus(Enum):
    NEW = 'N'
    CONSUMED = 'C'
    PROCESSING = 'P'