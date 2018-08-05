import os
import glob
import logging
import re
import datetime

from fr_user_event_consumer.log_file import LogFile
from fr_user_event_consumer.event_type import EventType

TIMESTAMP_FORMAT = '%Y%m%d-%H%M%S'
logger = logging.getLogger( __name__ )

def find_files_to_consume( event_type, timestamp_pattern, directory, file_glob,
    from_timestamp = None, to_timestamp = None, sample_rate_pattern = None ):

    if not os.path.isdir( directory ):
        raise ValueError( f'Not a directory: {directory}')

    if os.path.dirname( file_glob ):
        raise ValueError( f'file_glob can\'t include directory: {file_glob}' )

    # Find subdirectories but don't follow symlinks, in case infinite recursion
    directories = [ x[0] for x in os.walk( directory, followlinks = False ) ]

    # Regex pattern for extracting timestamps from filenames
    ts_pattern = re.compile( timestamp_pattern )

    # For CentralNotice events, require sample rate pattern, too
    if event_type == EventType.CENTRAL_NOTICE:
        if not sample_rate_pattern:
            raise ValueError(
                'Sample rate pattern is required for CentralNotice events.' )

        sr_pattern = re.compile( sample_rate_pattern )

    # Check for duplicate filenames (since we're looking in subdirectories, too)
    filenames = []
    files = []
    for d in directories:
        filenames_in_dir = glob.glob( os.path.join( d, file_glob ) )

        for fn in filenames_in_dir:
            base_fn = os.path.basename( fn )
            fn_ts = ts_pattern.search( base_fn ).group( 0 )

            # Duplicate filenames not allowed, regardless of directory
            if base_fn in filenames:
                raise ValueError(
                    f'Duplicate filename found: {base_fn} in {d}' )

            if ( from_timestamp is not None ) and ( fn_ts < from_timestamp ):
                continue

            if ( to_timestamp is not None ) and ( fn_ts > to_timestamp ):
                continue

            filenames.append( base_fn )
            timestamp = datetime.datetime.strptime( fn_ts, TIMESTAMP_FORMAT )

            if event_type == EventType.LANDING_PAGE:
                files.append( LogFile(
                    filename = base_fn,
                    directory = d,
                    timestamp = timestamp,
                    event_type = EventType.LANDING_PAGE
                ) )

            elif event_type == EventType.CENTRAL_NOTICE:

                sample_rate = int( sr_pattern.search( base_fn ).group( 0 ) )

                if ( sample_rate <= 0 ) or ( sample_rate > 100 ):
                    raise ValueError(
                        f'Invalid sample rate {sample_rate} for {base_fn}.' )

                files.append( LogFile(
                    filename = base_fn,
                    directory = d,
                    timestamp = timestamp,
                    event_type = EventType.CENTRAL_NOTICE,
                    sample_rate = sample_rate
                ) )

            else:
                raise ValueError( 'Incorrect value for event_type' )

    logger.debug(
        f'Found {len( files )} file(s) in {len( directories )} directorie(s)' )

    return files


def lines( file ):
    filename = os.path.join( file.directory, file.filename )
    with open( filename ) as stream:
        for l in stream:
            yield l
