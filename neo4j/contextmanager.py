from contextlib import contextmanager
from neo4j.connection import Connection


class Neo4jDBConnectionManager():

    """
    Every new connection is a transaction. To minimize new connection overhead for many reads we try to reuse a single
    connection. If this seem like a bad idea some kind of connection pool might work better.

    Neo4jDBConnectionManager.read()
    When using with Neo4jDBConnectionManager.read(): we will always rollback the transaction. All exceptions will be
    thrown.

    Neo4jDBConnectionManager.write()
    When using with Neo4jDBConnectionManager.write() we will always commit the transaction except when we see an
    exception. If we get an exception we will rollback the transaction and throw the exception.

    Neo4jDBConnectionManager.transaction()
    When we don't want to share a connection (transaction context) we can set up a new connection which will work
    just as the write context manager above but with it's own connection.

    >>> manager = Neo4jDBConnectionManager("http://localhost:7474")
    >>> with manager.write() as w:
    ...     w.execute("CREATE (TheMatrix:Movie {title:'The Matrix', tagline:'Welcome to the Real World'})")
    ...
    <neo4j.cursor.Cursor object at 0xb6fafa4c>
    >>>
    >>> with manager.read() as r:
    ...     for n in r.execute("MATCH (n:Movie) RETURN n LIMIT 1"):
    ...         print n
    "({'tagline': 'Welcome to the Real World', 'title': 'The Matrix'},)"

    Commits in batches can be achieved by:
    >>> with manager.write() as w:
    ...     w.execute("CREATE (TheMatrix:Movie {title:'The Matrix Reloaded', tagline:'Free your mind.'})")
    ...     w.connection.commit()  # The Matric Reloaded will be committed
    ...     w.execute("CREATE (TheMatrix:Movie {title:'Matrix Revolutions', tagline:'Everything that has a beginning has an end.'})")
    """

    def __init__(self, dsn):
        self.dsn = dsn
        self.connection = Connection(dsn)

    @contextmanager
    def _read(self):
        try:
            yield self.connection.cursor()
        finally:
            self.connection.rollback()
    read = property(_read)

    @contextmanager
    def _write(self):
        try:
            yield self.connection.cursor()
        except Connection.Error as e:
            self.connection.rollback()
            raise e
        else:
            self.connection.commit()
        finally:
            pass
    write = property(_write)

    @contextmanager
    def _transaction(self):
        connection = Connection(self.dsn)
        try:
            yield connection.cursor()
        except Connection.Error as e:
            connection.rollback()
            raise e
        else:
            connection.commit()
        finally:
            connection.close()
    transaction = property(_transaction)
