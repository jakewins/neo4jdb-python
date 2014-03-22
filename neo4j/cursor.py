
import neo4j

class Cursor(object):

    def __init__( self, connection, execute_statements ):
        self.connection = connection
        self.lastrowid = None
        self.messages = []
        self.arraysize = 1
        self.description = None
        self.errorhandler = connection.errorhandler

        self._execute = execute_statements
        self._rows = None
        self._rowcount = -1
        self._cursor = 0

    def execute( self, statement, *args, **kwargs ):
        for i in range(len(args)):
            kwargs[i] = args[i]

        self.messages = []
        result = self._execute( self, [( statement, kwargs )] )
        self._rows = result['data']
        self._rowcount = len(self._rows)
        self.description = [ (name, neo4j.MIXED, None, None, None, None, True) for name in result['columns'] ] 
        return self

    def fetchone(self):
        row = self._rows[self._cursor]
        self._cursor += 1
        return tuple(row['row'])

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize
        result = [ tuple(r['row']) for r in self._rows[self._cursor:self._cursor + size] ]
        self._cursor += size
        return result

    def fetchall(self):
        result = [ tuple(r['row']) for r in self._rows[self._cursor:] ]
        self._cursor += self.rowcount
        return result

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return self.fetchone()
        except IndexError:
            raise StopIteration()

    def next(self):
        return self.__next__()

    def scroll(self, value, mode='relative'):
        if value < 0:
            raise connection.NotSupportedError()
        if mode == 'relative':
            self._cursor += value
        elif mode == 'absolute':
            self._cursor = value

        if self._cursor >= self.rowcount:
            self._cursor = self.rowcount
            raise IndexError()

    @property
    def rowcount(self):
        return self._rowcount

    def nextset( self ):
        pass

    def setinputsizes(self, sizes):
        pass

    def setoutputsizes(self, size, column=None):
        pass

    def close(self):
        self._result = None
        self._rows = None
        self._rowcount = -1
        self.messages = []
        self.description = None        