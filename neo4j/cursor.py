
import neo4j
from neo4j.strings import ustr


class Cursor(object):

    def __init__( self, cursorid, connection, execute_statements ):
        self.connection = connection
        self.lastrowid = None
        self.arraysize = 1
        self.errorhandler = connection.errorhandler

        self._id = cursorid

        self._pending = []
        self._execute = execute_statements
        self._rows = None
        self._rowcount = -1
        self._cursor = 0
        self._messages = []

    def execute(self, statement, *args, **kwargs):
        for i in range(len(args)):
            kwargs[i] = args[i]

        self._messages = []
        self._rows = None
        self._rowcount = 0

        self._pending.append((statement, kwargs))
        return self

    def fetchone(self):
        self._execute_pending()
        row = self._rows[self._cursor]
        self._cursor += 1
        return self._map_row(row['rest'])

    def fetchmany(self, size=None):
        self._execute_pending()
        if size is None:
            size = self.arraysize
        result = [self._map_row(r['rest']) for r in self._rows[self._cursor:self._cursor + size]]
        self._cursor += size
        return result

    def fetchall(self):
        self._execute_pending()
        result = [self._map_row(r['rest']) for r in self._rows[self._cursor:]]
        self._cursor += self.rowcount
        return result

    def __iter__(self):
        self._execute_pending()
        return self

    def __next__(self):
        try:
            return self.fetchone()
        except IndexError:
            raise StopIteration()

    def next(self):
        return self.__next__()

    def scroll(self, value, mode='relative'):
        self._execute_pending()
        if value < 0:
            raise self.connection.NotSupportedError()
        if mode == 'relative':
            self._cursor += value
        elif mode == 'absolute':
            self._cursor = value

        if self._cursor >= self.rowcount:
            self._cursor = self.rowcount
            raise IndexError()

    @property
    def description(self):
        self._execute_pending()
        return self._description

    @property
    def rowcount(self):
        self._execute_pending()
        return self._rowcount

    @property
    def messages(self):
        self._execute_pending()
        return self._messages

    def nextset(self):
        pass

    def setinputsizes(self, sizes):
        pass

    def setoutputsizes(self, size, column=None):
        pass

    def close(self):
        self._rows = None
        self._rowcount = -1
        self._messages = []
        self._description = None
        self.connection._cursors.discard(self)

    def __del__(self):
        self.close()

    def __hash__(self):
        return self._id

    def __eq__(self, other):
        return self._id == other._id

    def _map_row(self, row):
        return tuple(self._map_value(row))

    def _map_value(self, value):
        ''' Maps a raw deserialized row to proper types '''
        # TODO: Once we've gotten here, we've done the following:
        # -> Recieve the full response, copy it from network buffer it into a ByteBuffer (copy 1)
        # -> Copy all the data into a String (copy 2)
        # -> Deserialize that string (copy 3)
        # -> Map the deserialized JSON to our response format (copy 4, what we are doing in this method)
        # This should not bee needed. Technically, this mapping from transport format to python types should require exactly one copy, 
        # from the network buffer into the python VM.
        if isinstance(value, list):
            out = []
            for c in value:
                out.append(self._map_value( c ))
            return out
        elif isinstance(value, dict) and 'metadata' in value and 'labels' in value['metadata'] and 'self' in value:
            return neo4j.Node(ustr(value['metadata']['id']), value['metadata']['labels'], value['data'])
        elif isinstance(value, dict) and 'metadata' in value and 'type' in value and 'self' in value:
            return neo4j.Relationship(ustr(value['metadata']['id']), value['type'], value['start'].split('/')[-1], value['end'].split('/')[-1], value['data'])
        elif isinstance(value, dict):
            out = {}
            for k,v in value.items():
                out[k] = self._map_value( v )
            return out
        elif isinstance(value, str):
            return ustr(value)
        else:
            return value


    def _execute_pending(self):
        if len(self._pending) > 0:
            pending = self._pending

            # Clear these, in case the request fails.
            self._pending = []
            self._rows = []
            self._rowcount = 0
            self._description = []

            result = self._execute(self, pending)

            self._rows = result['data']
            self._rowcount = len(self._rows)
            self._description = [(name, neo4j.MIXED, None, None, None, None, True) for name in result['columns']]
            self._cursor = 0
