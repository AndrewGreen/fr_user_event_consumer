FILE_KNOWN_SQL = 'SELECT EXISTS (SELECT 1 FROM files_processed WHERE filename = %s)'

class LogFileMapper:

    def __init__( self ):
        # This should be set by the caller before other methods are called
        self.connection = None

    def file_known( self, file ):
        cursor = self.connection.cursor()
        cursor.execute( FILE_KNOWN_SQL, ( file.filename, ) )
        return bool( cursor.fetchone()[ 0 ] )
