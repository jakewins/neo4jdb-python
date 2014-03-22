
import json

from neo4j.cursor import Cursor

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


    def __init__(self, dbUri):
        self.messages = []
        self.errorhandler = default_error_handler
        self._http = http.HTTPConnection(urlparse(dbUri).netloc)
        self._tx  = TX_ENDPOINT

    def commit(self):
        self.messages = []
        if self._tx != TX_ENDPOINT:
            self._http.request("POST", self._tx + "/commit" )
            self._tx = TX_ENDPOINT
            response = self._deserialize( self._http.getresponse() )
            self._handle_errors(response, self, None)

    def rollback(self):
        self.messages = []
        if self._tx != TX_ENDPOINT:
            self._http.request("DELETE", self._tx )
            self._tx = TX_ENDPOINT
            response = self._deserialize( self._http.getresponse() )
            self._handle_errors(response, self, None)

    def cursor(self):
        self.messages = []
        return Cursor( self, self._execute )

    def close(self):
        self.messages = []
        if hasattr(self, '_http') and self._http != None:
            self._http.close()
            self._http = None

    def __del__(self):
        self.close()

    def _handle_errors(self, response, owner, cursor):
        for error in response['errors']:
            ErrorClass = neo_code_to_error_class(error['code'])
            error_value = error['code'] + ": " + error['message']
            owner.messages.append( ( ErrorClass, error_value))
            owner.errorhandler(self, cursor, ErrorClass, error_value)

    def _execute( self, cursor, statements ):
        '''
        Executes a list of statements, returning an iterator of results sets. Each 
        statement should be a tuple of (statement, params).
        '''
        payload = [ { 'statement':s, 'parameters':p } for (s, p) in statements ]
        self._http.request("POST", self._tx, json.dumps( {'statements':payload} ) )
        
        http_response = self._http.getresponse()

        if self._tx == TX_ENDPOINT:
            self._tx = http_response.getheader('Location')

        response = self._deserialize( http_response )
        self._handle_errors(response, cursor, cursor)

        return response['results'][-1]

    def _deserialize(self, response):
        r = json.loads(response.read().decode('utf-8'))
        print(r)
        return r