
from neo4j.connection import Connection

apilevel = '2.0'
threadsafety = 1

# This is non-standard, it uses neos built-in params. 
paramstyle = 'curly'

def connect( dsn ):
    return Connection( dsn )

#
# Types
# Neo4j is a schema-optional database, which means that
# result rows can contain arbitrary types, and the database
# does not know in advance what types it will be dealing with.
# Because of this, we always describe return types as neo4j.MIXED,
# and we don't currently allow using the richer type set defined
# by the spec, since the transport format is JSON. This will change
# when the transport format changes.
#

class TypeCode(object):
    
    def __init__(self, code):
        self._code = code

    def __eq__(self, other):
        return other is self

    def __unicode__(self):
        return self._code

    def __repr__(self):
        return self._code

    def __str__(self):
        return self._code

class TypeObject(object):
    
    def __init__(self, *args, **kwargs):
        raise NotSupportedError("Complex types are not yet supported.")

Date               = TypeObject
Time               = TypeObject
Timestamp          = TypeObject
DateFromTicks      = TypeObject
TimeFromTicks      = TypeObject
TimestampFromTicks = TypeObject
Binary             = TypeObject

STRING   = TypeCode( "STRING"   )
BINARY   = TypeCode( "BINARY"   ) 
NUMBER   = TypeCode( "NUMBER"   )
DATETIME = TypeCode( "DATETIME" )
ROWID    = TypeCode( "ROWID"    )

MIXED      = TypeCode( "MIXED" )

#
# Exceptions 
#

Error             = Connection.Error
Warning           = Connection.Warning
InterfaceError    = Connection.InterfaceError
DatabaseError     = Connection.DatabaseError
InternalError     = Connection.InternalError
OperationalError  = Connection.OperationalError
ProgrammingError  = Connection.ProgrammingError
IntegrityError    = Connection.IntegrityError
DataError         = Connection.DataError
NotSupportedError = Connection.NotSupportedError