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

        # Open db connection
        db.connect( **self._db_settings )

        # TODO Check no files are in partially processed state

        # For from_latest option, get the most recent time of all consumed files
        if self._from_latest:
            # TODO
            pass

        # Get a list of objects with info about files to try
        file_infos = log_file_manager.file_infos(
            timestamp_format = self._timestamp_format_in_filenames,
            extract_timetamp_regex = self._extract_timestamp_regex,
            directory = self._directory,
            file_glob = self._file_glob,
            from_time = self._from_time,
            to_time = self._to_time
        )

        self._stats[ 'files_to_consume' ] = 0
        self._stats[ 'files_skipped' ] = 0

        for file_info in file_infos:
            filename = file_info[ 'filename' ]
            directory = file_info[ 'directory' ]

            # Skip any files already known to the db
            if db.log_file_mapper.file_known( filename ):
                logger.debug( f'Skipping already processed {filename}.')
                self._stats[ 'files_skipped' ] += 1
                continue

            logger.debug( f'Processing {filename}.' )
            self._stats[ 'files_to_consume' ] += 1

            sample_rate = log_file_manager.sample_rate( filename,
                self._extract_sample_rate_regex )

            # Create a new file object and insert it in the database
            file = db.log_file_mapper.new_file(
                filename = filename,
                directory = directory,
                time = file_info[ 'time' ],
                event_type = EventType.CENTRAL_NOTICE,
                sample_rate = sample_rate,
                status = LogFileStatus.PROCESSING
            )

            # Count events consumed, events ignored and invalid lines for this file
            consumed_events = 0
            ignored_events = 0
            invalid_lines = 0

            # Cycle through the lines in the file, create and aggregate the events
            for line, line_no in log_file_manager.lines( file ):
                event = CentralNoticeEvent( line )

                # Events arrive via a public URL. Some validation happens before they
                # get here, but we do a bit more.
                if not event.valid:
                    invalid_lines += 1
                    logger.debug(
                        f'Invalid data on line {line_no} of {file.filename}: {line}' )

                    continue

                # Ignore events from declared bots
                if event.is_bot:
                    ignored_events += 1
                    continue

                consumed_events += 1

            # Set file's stats and status as consumed, and save
            file.consumed_events = consumed_events
            file.ignored_events = ignored_events
            file.invalid_lines = invalid_lines
            file.status = LogFileStatus.CONSUMED
            db.log_file_mapper.save_file( file )

        db.close()

