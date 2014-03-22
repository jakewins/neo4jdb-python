import unittest

import neo4j

class TestConnection(unittest.TestCase):

    def setUp(self):
        self.conn = neo4j.connect("http://localhost:7474")

    def test_commit(self):
        # Given
        cursor = self.conn.cursor()
        cursor.execute("CREATE (n:TestCommit {name:1337})")

        # When
        self.conn.commit()

        # Then other cursors should see it
        cursor = self.conn.cursor()
        cursor.execute("MATCH (n:TestCommit) RETURN n.name")
        self.assertEqual(cursor.fetchone(), (1337,))

        # And other connections should see it
        cursor = neo4j.connect("http://localhost:7474").cursor()
        cursor.execute("MATCH (n:TestCommit) RETURN n.name")
        self.assertEqual(cursor.fetchone(), (1337,))

    def test_read_and_commit(self):
        # Given
        cursor = self.conn.cursor()
        cursor.execute("CREATE (n:TestCommit {name:1337})")
        cursor.rowcount # Force client to execute

        # When
        self.conn.commit()

        # Then other cursors should see it
        cursor = self.conn.cursor()
        cursor.execute("MATCH (n:TestCommit) RETURN n.name")
        self.assertEqual(cursor.fetchone(), (1337,))

        # And other connections should see it
        cursor = neo4j.connect("http://localhost:7474").cursor()
        cursor.execute("MATCH (n:TestCommit) RETURN n.name")
        self.assertEqual(cursor.fetchone(), (1337,))

    def test_rollback(self):
        # Given
        cursor = self.conn.cursor()
        cursor.execute("CREATE (n:TestRollback {name:1337})")

        # When
        self.conn.rollback()

        # Then the same cursor should not see it
        cursor.execute("MATCH (n:TestRollback) RETURN n.name")
        self.assertEqual(cursor.rowcount, 0)

        # And other connections should see it
        cursor = neo4j.connect("http://localhost:7474").cursor()
        cursor.execute("MATCH (n:TestRollback) RETURN n.name")
        self.assertEqual(cursor.rowcount, 0)


if __name__ == '__main__':
    unittest.main()