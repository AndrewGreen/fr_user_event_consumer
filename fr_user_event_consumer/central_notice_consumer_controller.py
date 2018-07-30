import logging

from fr_user_event_consumer.log_file_manager import LogFileManager
from fr_user_event_consumer.db import CentralNoticeEventMapper, LogFileMapper

logger = logging.getLogger( __name__ )

class CentralNoticeConsumerController:

    def __init__( self, timestamp_pattern, sample_rate_pattern, directory,
        file_glob, from_latest = False, from_timestamp = None,
        to_timestamp = None ):

        if ( from_latest and from_timestamp ):
            raise ValueError( 'Can\'t set both from_latest and from_timestamp.' )

        self._timestamp_pattern = timestamp_pattern
        self._sample_rate_pattern = sample_rate_pattern
        self._directory = directory
        self._file_glob = file_glob
        self._from_latest = from_latest
        self._from_timestamp = from_timestamp
        self._to_timestamp = to_timestamp

        self._log_file_manager = LogFileManager( LogFileMapper() )
        self._central_notice_event_mapper = CentralNoticeEventMapper()


    def execute( self ):


        # For from_latest option, get the most recent timestamp of all consumed files
        if ( self._from_latest ):
            # TODO
            pass

        logger.info(
            f"Looking for files to consume with {self._file_glob} in {self._directory} "
            f"from {self._from_timestamp} to {self._to_timestamp}."
        )

        # Get a list of log files to process
        files = self._log_file_manager.find_files(
            self._timestamp_pattern,
            self._directory,
            self._file_glob,
            self._from_timestamp,
            self._to_timestamp
        )


