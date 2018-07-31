import os
import glob
import logging
import re

from fr_user_event_consumer.log_file import LogFile

TIMESTAMP_FORMAT = '%Y%m%d-%H%M%S'
logger = logging.getLogger( __name__ )

class LogFileManager:

    def find_files_to_consume( self, timestamp_pattern, directory, file_glob,
        from_timestamp = None, to_timestamp = None ):

        if ( not os.path.isdir( directory ) ):
            raise ValueError( f'Not a directory: {directory}')

        if ( os.path.dirname( file_glob ) ):
            raise ValueError( f'file_glob can\'t include directory: {file_glob}' )

        # Find subdirectories but don't follow symlinks, in case infinite recursion
        directories = [ x[0] for x in os.walk( directory, followlinks = False ) ]

        # Regex pattern for extracting timestamps from filenames
        ts_pattern = re.compile( timestamp_pattern )

        # Check for duplicate filenames (since we're looking in subdirectories, too)
        filenames = []
        files = []
        for d in directories:
            filenames_in_dir = glob.glob( os.path.join( d, file_glob ) )

            for fn in filenames_in_dir:
                fn_ts = ts_pattern.search( fn ).group( 0 )

                # Duplicate filenames not allowed, regardless of directory
                if ( fn in filenames ):
                    raise ValueError( f'Duplicate filename found: {fn}' )

                if ( ( from_timestamp is not None ) and ( fn_ts < from_timestamp ) ):
                    continue

                if ( ( to_timestamp is not None ) and ( fn_ts > to_timestamp ) ):
                    continue

                filenames.append( fn )
                files.append( LogFile( os.path.basename( fn ), d, fn_ts ) )

        logger.debug(
            f'Found {len( files )} files in {len( directories )} directories' )

        return files
