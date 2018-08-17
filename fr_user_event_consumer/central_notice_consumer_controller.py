import logging

from fr_user_event_consumer.event_type import EventType
from fr_user_event_consumer.log_file import LogFileStatus

from fr_user_event_consumer import log_file_manager
from fr_user_event_consumer import db

_logger = logging.getLogger( __name__ )

def consume_events(
    db_settings,
    timestamp_format_in_filenames,
    extract_timestamp_regex,
    extract_sample_rate_regex,
    directory,
    file_glob,
    detail_languages,
    detail_projects_regex,
    from_latest = False,
    from_time = None,
    to_time = None
):

    if from_latest and from_time:
        raise ValueError( 'Can\'t set both from_latest and from_time.' )

    stats = {}

    # Open db connection
    db.connect( **db_settings )

    # Check no files are in partially processed state
    if db.log_file_mapper.files_with_processing_status( EventType.CENTRAL_NOTICE ):
        raise RuntimeError(
            'Files with processing status found. A previous execution was probably '
            'interrupted before completion. Back up the database and purge '
            'data from incomplete processing.'
        )

    # For from_latest option, get the most recent time of all consumed files
    if from_latest:
        from_time = db.log_file_mapper.get_lastest_time()
        if from_time is None:
            _logger.warn(
                'Requested processing files from latest time previously consumed, '
                'but no latest time was found. Processing with no \'from\' limit' )

    # Get a list of objects with info about files to try
    file_infos = log_file_manager.file_infos(
        timestamp_format = timestamp_format_in_filenames,
        extract_timetamp_regex = extract_timestamp_regex,
        directory = directory,
        file_glob = file_glob,
        from_time = from_time,
        to_time = to_time
    )

    stats[ 'consumed_files' ] = 0
    stats[ 'skipped_files' ] = 0

    for file_info in file_infos:
        filename = file_info[ 'filename' ]
        directory = file_info[ 'directory' ]

        # Skip any files already known to the db
        if db.log_file_mapper.known( filename ):
            _logger.debug( 'Skipping already processed {}.'.format( filename ) )
            stats[ 'skipped_files' ] += 1
            continue

        _logger.debug( 'Processing {}.'.format( filename ) )

        sample_rate = log_file_manager.sample_rate( filename, extract_sample_rate_regex )

        stats[ 'consumed_events' ] = 0
        stats[ 'ignored_events' ] = 0
        stats[ 'invalid_lines' ] = 0

        # Create a new file object and insert it in the database
        file = db.log_file_mapper.new(
            filename = filename,
            directory = directory,
            time = file_info[ 'time' ],
            event_type = EventType.CENTRAL_NOTICE,
            sample_rate = sample_rate,
            status = LogFileStatus.PROCESSING
        )

        _process_file( file, detail_languages, detail_projects_regex )

        stats[ 'consumed_files' ] += 1

        stats[ 'consumed_events' ] += file.consumed_events
        stats[ 'ignored_events' ] += file.ignored_events
        stats[ 'invalid_lines' ] += file.invalid_lines

    db.close()
    return stats


def purge_incomplete( db_settings ):
    _logger.debug( 'Purging data and file records for files with processing status.' )
    db.connect( **db_settings )

    cell_count = db.central_notice_event_aggregator.delete_with_processing_status()

    _logger.debug( 'Removed {} data cells from files with processing status.'
        .format( cell_count ) )

    file_count = db.log_file_mapper.delete_with_processing_status(
        EventType.CENTRAL_NOTICE )

    _logger.debug( 'Removed {} files with processing status.'.format( file_count ) )

    db.close()


def _process_file( file, detail_languages, detail_projects_regex ):

        # Count events consumed, events ignored and invalid lines for this file
        consumed_events = 0
        ignored_events = 0
        invalid_lines = 0

        # Start aggregation step (to aggregate data per-file)
        aggregation_step = db.central_notice_event_aggregator.new_cn_aggregation_step(
            detail_languages,
            detail_projects_regex,
            file
        )

        # Cycle through the lines in the file, create and aggregate the events
        for line, line_no in log_file_manager.lines( file ):
            event = db.central_notice_event_aggregator.new_unsaved( line )

            # Events arrive via a public URL. Some validation happens before they
            # get here, but we do a bit more.
            if not event.valid:
                invalid_lines += 1
                _logger.debug( 'Invalid data on line {} of {}: {}'.format(
                    line_no, file.filename, line ) )

                continue

            # Ignore events from declared bots, previews or banner not shown
            if event.bot or event.testing or ( not event.banner_shown ):
                ignored_events += 1
                continue

            aggregation_step.add_event( event )
            consumed_events += 1

        # Finish the aggregation (inserts aggregated data from the file in the db)
        aggregation_step.save()

        # Set file's stats and status as consumed, and save
        file.consumed_events = consumed_events
        file.ignored_events = ignored_events
        file.invalid_lines = invalid_lines
        file.status = LogFileStatus.CONSUMED

        db.log_file_mapper.save( file )
