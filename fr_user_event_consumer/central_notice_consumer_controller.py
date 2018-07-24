import logging
from datetime import datetime, timedelta

from fr_user_event_consumer.log_file_manager import LogFileManager
from fr_user_event_consumer.db import CentralNoticeEventMapper, LogFileMapper

logger = logging.getLogger( __name__ )

class CentralNoticeConsumerController:

    def __init__( self, directory, filename_formatter, file_glob = None,
        since_last = False, last_hours = None ):

        # Required arguments
        self._directory = directory
        self._filename_formatter = filename_formatter

        # Require exactly one of file_glob, since_last or last_hours
        if ( int( file_glob is not None ) +
            int( since_last ) +
            int( last_hours is not None ) != 1 ):
            raise ValueError( 'Require exactly one of: file_glob, since_last, last_hours' )

        self._file_glob = file_glob
        self._since_last = since_last
        self._last_hours = last_hours

        self._log_file_manager = LogFileManager( LogFileMapper() )
        self._central_notice_event_mapper = CentralNoticeEventMapper()


    def execute( self ):

        start_time = datetime.now()

        # Get a list of log files to process

        if ( self._file_glob is not None ):
            files = self._log_file_manager.list_files_from_glob( self._directory,
                self._file_glob )

        elif( self._since_last ):
            files = self._log_file_manager.list_files_since_last( self._directory,
                self._filename_formatter )

        elif ( self._last_hours is not None ):
            since = start_time - timedelta( hours = self._last_hours )
            files = self._log_file_manager.list_files_since_time( self._directory,
                self._filename_formatter, since )

        else:
            # Should never get here
            raise ValueError( 'Incorrect setup, no file selection criteria provided.' )

        for file in files:
            print( file )
