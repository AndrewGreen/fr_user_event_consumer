import logging

from fr_user_event_consumer.log_file_manager import LogFileManager
from fr_user_event_consumer.impression_type import ImpressionType
from fr_user_event_consumer.log_file_status import LogFileStatus
import fr_user_event_consumer.db as db

logger = logging.getLogger( __name__ )

class CentralNoticeConsumerController:

    def __init__( self, db_settings, timestamp_pattern, sample_rate_pattern, directory,
        file_glob, from_latest = False, from_timestamp = None,
        to_timestamp = None ):

        if ( from_latest and from_timestamp ):
            raise ValueError( 'Can\'t set both from_latest and from_timestamp.' )

        self._db_settings = db_settings
        self._timestamp_pattern = timestamp_pattern
        self._sample_rate_pattern = sample_rate_pattern
        self._directory = directory
        self._file_glob = file_glob
        self._from_latest = from_latest
        self._from_timestamp = from_timestamp
        self._to_timestamp = to_timestamp

        self._log_file_manager = LogFileManager()
        self._log_file_mapper = db.LogFileMapper()
        self._central_notice_event_mapper = db.CentralNoticeEventMapper()

        self._stats = {}


    def execute( self ):

        # Set up db connection
        connection = db.connect( **self._db_settings )
        self._log_file_mapper.connection = connection

        # TODO Check no files are in partially processed state

        # For from_latest option, get the most recent timestamp of all consumed files
        if ( self._from_latest ):
            # TODO
            pass

        # Get the files to try
        files = self._log_file_manager.find_files_to_consume(
            self._timestamp_pattern,
            self._directory,
            self._file_glob,
            self._from_timestamp,
            self._to_timestamp
        )

        self._stats[ 'files_found' ] = len( files )

        # Filter out files already known to the database
        files = [ f for f in files if not self._log_file_mapper.file_known( f ) ]

        self._stats[ 'files_to_consume' ] = len( files )

        logger.debug(
            f'Skipping {self._stats[  "files_found" ] - self._stats[ "files_to_consume" ]} '
            'file(s) already consumed.'
        )

        for file in files:
            logger.debug( f'Processing {file.filename}.' )
            file.status = LogFileStatus.PROCESSING
            file.impression_type = ImpressionType.BANNER
            self._log_file_mapper.save_file( file )

        db.close()

