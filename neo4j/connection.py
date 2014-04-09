
import json

from neo4j.cursor import Cursor
from neo4j.strings import ustr

try:
    from http import client as http
    from urllib.parse import urlparse
    StandardError = Exception
except:
    import httplib as http
    from urlparse import urlparse
    from exceptions import StandardError

TX_ENDPOINT = "/db/data/transaction"

def neo_code_to_error_class(code):
    if code.startswith('Neo.ClientError.Schema'):
        return Connection.IntegrityError
    elif code.startswith('Neo.ClientError'):
        return Connection.ProgrammingError
    return Connection.InternalError

def default_error_handler(connection, cursor, errorclass, errorvalue):
    if errorclass != Connection.Warning:
        raise errorclass(errorvalue)

class Connection(object):

    class Error(StandardError):
        pass

    class Warning(StandardError):
        pass

    class InterfaceError(Error):
        pass

    class DatabaseError(Error):
        pass

    class InternalError(DatabaseError):
        pass

    class OperationalError(DatabaseError):
        pass

    class ProgrammingError(DatabaseError):
        pass

    class IntegrityError(DatabaseError):
        pass

    class DataError(DatabaseError):
        pass

    class NotSupportedError(DatabaseError):
        pass 

    _COMMON_HEADERS = {"Content-Type":"application/json", "Accept":"application/json", "Connection":"keep-alive"}

    def __init__(self, dbUri):
        self.errorhandler = default_error_handler
        self._http = http.HTTPConnection(urlparse(dbUri).netloc)
        self._tx  = TX_ENDPOINT
        self._messages = []
        self._cursors = set()
        self._cursor_ids = 0

    def commit(self):
        self._messages = []
        pending = self._gather_pending()
        
        if self._tx != TX_ENDPOINT or len(pending) > 0:
            payload = None
            if len(pending) > 0:
                payload = {'statements':[ { 'statement':s, 'parameters':p } for (s, p) in pending ]}
            response = self._deserialize( self._http_req("POST", self._tx + "/commit", payload) )
            self._tx = TX_ENDPOINT
            self._handle_errors(response, self, None)

    def rollback(self):
        self._messages = []
        self._gather_pending() # Just used to clear all pending requests
        if self._tx != TX_ENDPOINT:
            self._tx = TX_ENDPOINT
            response = self._deserialize( self._http_req("DELETE", self._tx) )
            self._handle_errors(response, self, None)

    def cursor(self):
        self._messages = []
        cursor = Cursor( self._next_cursor_id(), self, self._execute )
        self._cursors.add(cursor)
        return cursor

    def close(self):
        self._messages = []
        if hasattr(self, '_http') and self._http != None:
            self._http.close()
            self._http = None

    def __del__(self):
        self.close()

    @property
    def messages(self):
        return self._messages

    def _next_cursor_id(self):
        self._cursor_ids += 1
        return self._cursor_ids

    def _gather_pending(self):
        pending = []
        for cursor in self._cursors:
            if len(cursor._pending) > 0:
                pending.extend(cursor._pending)
                cursor._pending = []
        return pending

    def _handle_errors(self, response, owner, cursor):
        for error in response['errors']:
            ErrorClass = neo_code_to_error_class(error['code'])
            error_value = error['code'] + ": " + ustr(error['message'])
            owner._messages.append( ( ErrorClass, error_value))
            owner.errorhandler(self, cursor, ErrorClass, error_value)

    def _execute( self, cursor, statements ):
        '''
        Executes a list of statements, returning an iterator of results sets. Each 
        statement should be a tuple of (statement, params).
        '''
        payload = [ { 'statement':s, 'parameters':p } for (s, p) in statements ]
        
        http_response = self._http_req("POST", self._tx, {'statements':payload})
       
        if self._tx == TX_ENDPOINT:
            self._tx = http_response.getheader('Location')
        
        response = self._deserialize( http_response )
        self._handle_errors(response, cursor, cursor)
        
        return response['results'][-1]

    def _http_req(self, method, path, payload=None):
        payload = json.dumps(payload) if payload is not None else None
        self._http.request("POST", self._tx, payload, self._COMMON_HEADERS)
        
        try:
            http_response = self._http.getresponse() 
        except http.BadStatusLine:
            raise Connection.OperationalError("This connection has expired.")

        if not http_response.status in [200, 201]:
            raise Connection.OperationalError("Server returned unexpected response: " + ustr(http_response.status) + ustr(http_response.read()))
        
        return http_response

    def _deserialize(self, response):
        # TODO: This is exceptionally annoying, python 3 has improved byte array handling, but that means the JSON parser
        # no longer supports deserializing these things in a streaming manner, so we have to decode the whole thing first.
        return json.loads(response.read().decode('utf-8'))
