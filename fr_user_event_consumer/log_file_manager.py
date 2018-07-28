import os
import glob
import logging
import re

TIMESTAMP_FORMAT = '%Y%m%d-%H%M%S'
logger = logging.getLogger( __name__ )

class LogFileManager:

    def __init__( self, log_file_mapper ):
        self._log_file_mapper = log_file_mapper


    def list_files( self, timestamp_pattern, directory, file_glob, from_timestamp = None,
        to_timestamp = None ):

        if ( not os.path.isdir( directory ) ):
            raise ValueError( f'Not a directory: {directory}')

        if ( os.path.dirname( file_glob ) ):
            raise ValueError( f'file_glob can\'t include directory: {file_glob}' )

        # Find subdirectories but don't follow symlinks, in case infinite recursion
        directories = [ x[0] for x in os.walk( directory, followlinks = False ) ]

        # Regex pattern for extracting timestamps from filenames
        ts_pattern = re.compile( timestamp_pattern )

        files = []
        for d in directories:
            files_in_dir = glob.glob( os.path.join( d, file_glob ) )

            files_to_add = []
            for f in files_in_dir:
                f_ts = ts_pattern.search( f ).group( 0 )

                if ( ( from_timestamp is not None ) and ( f_ts < from_timestamp ) ):
                    continue

                if ( ( to_timestamp is not None ) and ( f_ts > to_timestamp ) ):
                    continue

                files_to_add.append( f )

            logger.debug( f'Found {len(files_to_add)} files in directory {d}' )
            files.extend( files_to_add )

        print ( files )
        return files

