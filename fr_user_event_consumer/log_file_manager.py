import os
import glob
import logging

TIMESTAMP_FORMAT = '%Y%m%d-%H%M%S'
logger = logging.getLogger( __name__ )

class LogFileManager:

    def __init__( self, log_file_mapper ):
        self._log_file_mapper = log_file_mapper


    def list_files_from_glob( self, directory, file_glob ):

        directories = self._get_directories( directory )

        if ( os.path.dirname( file_glob ) ):
            raise ValueError( f'file_glob can\'t include directory: {file_glob}' )

        files = []
        for d in directories:
            files_in_dir = glob.glob( os.path.join( d, file_glob ) )
            logger.debug( f'Selected {len(files_in_dir)} files from directory {d}' )
            files.extend( files_in_dir )

        return files


    def list_files_since_last( self, directory, filename_formatter ):
        pass


    def list_files_since_time( self, directory, filename_formatter, since ):

        directories = self._get_directories( directory )
        since_formated = since.strftime( TIMESTAMP_FORMAT )
        since_filename = filename_formatter.format( ts = since_formated )

        files = []
        for d in directories:
            files_in_dir = sorted( glob.glob( d ), reverse = True )
            for f in files_in_dir:
                if ( since_filename > f ):
                    break
                files.append( f )

        logger.debug( f'Selected {len(files)} files since {since_formated}' )

        return files


    def _get_directories( self, directory ):
        """Return a list of directories that includes directory and subdirectories"""
        if ( not os.path.isdir( directory ) ):
            raise ValueError( f'Not a directory: {directory}')

        # Don't follow symlinks, in case infinite recursion
        return [ x[0] for x in os.walk( directory, followlinks = False ) ]
