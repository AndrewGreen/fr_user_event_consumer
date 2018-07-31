import os
import glob
import logging
import re
import datetime

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
        file_infos = []
        for d in directories:
            filenames_in_dir = glob.glob( os.path.join( d, file_glob ) )

            for fn in filenames_in_dir:
                base_fn = os.path.basename( fn )
                fn_ts = ts_pattern.search( base_fn ).group( 0 )

                # Duplicate filenames not allowed, regardless of directory
                if ( base_fn in filenames ):
                    raise ValueError(
                        f'Duplicate filename found: {base_fn} in {d}' )

                if ( ( from_timestamp is not None ) and ( fn_ts < from_timestamp ) ):
                    continue

                if ( ( to_timestamp is not None ) and ( fn_ts > to_timestamp ) ):
                    continue

                filenames.append( base_fn )
                timestamp = datetime.datetime.strptime( fn_ts, TIMESTAMP_FORMAT )
                file_infos.append( ( base_fn, d, timestamp ) )

        logger.debug(
            f'Found {len( file_infos )} file(s) in {len( directories )} directorie(s)' )

        return file_infos
