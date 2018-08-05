import logging

from fr_user_event_consumer.event_type import EventType
from fr_user_event_consumer.log_file import LogFileStatus
from fr_user_event_consumer.central_notice_event import CentralNoticeEvent

import fr_user_event_consumer.log_file_manager as log_file_manager
import fr_user_event_consumer.db as db

logger = logging.getLogger( __name__ )

class CentralNoticeConsumerController:

    def __init__( self, db_settings, timestamp_format_in_filenames,
        extract_timestamp_regex, extract_sample_rate_regex, directory, file_glob,
        from_latest = False, from_time = None, to_time = None ):

        if from_latest and from_time:
            raise ValueError( 'Can\'t set both from_latest and from_timestamp.' )

        self._db_settings = db_settings
        self._timestamp_format_in_filenames = timestamp_format_in_filenames
        self._extract_timestamp_regex = extract_timestamp_regex
        self._extract_sample_rate_regex = extract_sample_rate_regex
        self._directory = directory
        self._file_glob = file_glob
        self._from_latest = from_latest
        self._from_time = from_time
        self._to_time = to_time

        self._stats = {}


    def execute( self ):

        # Set up db connection
        connection = db.connect( **self._db_settings )
        db.log_file_mapper.connection = connection

        # TODO Check no files are in partially processed state

        # For from_latest option, get the most recent timestamp of all consumed files
        if self._from_latest:
            # TODO
            pass

        # Get the files to try
        files = log_file_manager.find_files_to_consume(
            EventType.CENTRAL_NOTICE,
            self._timestamp_format_in_filenames,
            self._extract_timestamp_regex,
            self._directory,
            self._file_glob,
            self._from_time,
            self._to_time,
            self._extract_sample_rate_regex
        )

        self._stats[ 'files_found' ] = len( files )

        # Filter out files already known to the database
        files = [ f for f in files if not db.log_file_mapper.file_known( f ) ]

        self._stats[ 'files_to_consume' ] = len( files )

        logger.debug(
            f'Skipping {self._stats[  "files_found" ] - self._stats[ "files_to_consume" ]} '
            'file(s) already consumed.'
        )

        for file in files:
            logger.debug( f'Processing {file.filename}.' )

            # Save the file with processing status
            file.status = LogFileStatus.PROCESSING
            db.log_file_mapper.save_file( file )

            # Count events consumed and invalid lines
            consumed_events = 0
            invalid_lines = 0

            # Cycle through the lines in the file, create and aggregate the events
            for line in log_file_manager.lines( file ):
                event = CentralNoticeEvent( line )

                if event.valid:
                    # TODO Consume event
                    consumed_events += 1
                else:
                    invalid_lines +=1
                    logger.debug( f'Invalid line in {file.filename}: {line}' )

            # Set file's stats and status as consumed, and save
            file.consumed_events = consumed_events
            file.invalid_lines = invalid_lines
            file.status = LogFileStatus.CONSUMED
            db.log_file_mapper.save_file( file )

        db.close()

