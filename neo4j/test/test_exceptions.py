import unittest

import neo4j
import json

class TestExceptions(unittest.TestCase):

    def setUp(self):
        self.conn = neo4j.connect("http://localhost:7474")

    def test_syntax_error(self):
        # Given
        cursor = self.conn.cursor()
        
        # When
        try:
            cursor.execute("this is not valid syntax")
            cursor.rowcount # Force client to talk to server
            raise Exception("Should not have reached here.")
        except neo4j.ProgrammingError as e:
            # Then
            self.assertEqual(str(e), "Neo.ClientError.Statement.InvalidSyntax: Invalid input \'t\': expected <init> (line 1, column 1)\n\"this is not valid syntax\"\n ^")
            self.assertEqual(cursor.messages, [(neo4j.ProgrammingError, "Neo.ClientError.Statement.InvalidSyntax: Invalid input \'t\': expected <init> (line 1, column 1)\n\"this is not valid syntax\"\n ^")])

    def test_cursor_clears_errors(self):
        # Given
        cursor = self.conn.cursor()
        try:
            cursor.execute("this is not valid syntax")
            cursor.rowcount # Force client to talk to server
        except neo4j.ProgrammingError as e:
            pass
        self.conn.rollback()

        # When
        cursor.execute("CREATE (n)")

        # Then
        self.assertEqual(cursor.messages, [])

if __name__ == '__main__':
    unittest.main()