import unittest

import neo4j

class TestCursor(unittest.TestCase):

    def setUp(self):
        self.conn = neo4j.connect("http://localhost:7474")

    def test_description(self):
        # Given
        cursor = self.conn.cursor()

        # When
        cursor.execute("CREATE (n) RETURN 1 AS hello, 'str' AS str, 3")

        # Then
        self.assertEqual(cursor.description, [\
            ('hello', neo4j.MIXED, None, None, None, None, True),\
            ('str',   neo4j.MIXED, None, None, None, None, True),\
            ('3',     neo4j.MIXED, None, None, None, None, True)])

    def test_positional_parameters(self):
        # Given
        cursor = self.conn.cursor()

        # When
        cursor.execute("CREATE (n:Params {name:{0}})", "Bob")

        # Then
        self.assertEqual(list(cursor.execute("MATCH (n:Params) RETURN n")),\
            [({'name': 'Bob'},)])

    def test_named_parameters(self):
        # Given
        cursor = self.conn.cursor()

        # When
        cursor.execute("CREATE (n:Params {name:{name}})", name="Bob")

        # Then
        self.assertEqual(list(cursor.execute("MATCH (n:Params) RETURN n")),\
            [({'name': 'Bob'},)])

    def test_fetch_one(self):
        # Given
        cursor = self.conn.cursor()

        # When
        cursor.execute("CREATE (n) RETURN 1,2,3")

        # Then
        self.assertEqual(cursor.rowcount, 1)
        self.assertEqual(cursor.fetchone(), (1,2,3))

    def test_fetch_many(self):
        # Given
        cursor = self.conn.cursor()

        # When
        cursor.execute("""FOREACH (n IN [1,2,3,4,5,6,7]| CREATE (:Test { id:n }))
                          WITH 1 AS p
                          MATCH (k:Test)
                          RETURN k.id AS id ORDER BY id""")

        # Then
        self.assertEqual(cursor.rowcount, 7)
        self.assertEqual(cursor.fetchmany(2), [(1,), (2,)])
        self.assertEqual(cursor.fetchmany(3), [(3,), (4,), (5,)])
        self.assertEqual(cursor.fetchmany(5), [(6,), (7,)])

    def test_fetch_all(self):
        # Given
        cursor = self.conn.cursor()

        # When
        cursor.execute("""FOREACH (n IN [1,2,3,4,5,6,7]| CREATE (:Test { id:n }))
                          WITH 1 AS p
                          MATCH (k:Test)
                          RETURN k.id AS id ORDER BY id""")

        # Then
        self.assertEqual(cursor.fetchone(), (1,))
        self.assertEqual(cursor.fetchall(), [(2,), (3,), (4,), (5,), (6,), (7,)])
        self.assertEqual(cursor.fetchall(), [])

    def test_iter(self):
        # Given
        cursor = self.conn.cursor()

        # When
        cursor.execute("""FOREACH (n IN [1,2,3,4,5,6,7]| CREATE (:Test { id:n }))
                          WITH 1 AS p
                          MATCH (k:Test)
                          RETURN k.id AS id ORDER BY id""")

        # Then
        cells = []
        for cell, in cursor:
            cells.append(cell)
        self.assertEqual(cells, [1,2,3,4,5,6,7])
        self.assertEqual(cursor.fetchall(), [])

    def test_scroll(self):
        # Given
        cursor = self.conn.cursor()
        cursor.execute("""FOREACH (n IN [1,2,3,4,5,6,7]| CREATE (:Test { id:n }))
                          WITH 1 AS p
                          MATCH (k:Test)
                          RETURN k.id AS id ORDER BY id""")
        # When
        cursor.scroll(3)

        # Then
        self.assertEqual(cursor.fetchone(), (4,))

        # And When
        cursor.scroll(5, 'absolute')

        # Then
        self.assertEqual(cursor.fetchone(), (6,))

    def test_scroll_overflow(self):
        # Given
        cursor = self.conn.cursor()
        cursor.execute("""CREATE (n) RETURN 1""")
        
        # When
        try:
            cursor.scroll(10)
            raise Exception("Should not have reached here.")
        except IndexError as e:
            # Then
            pass


if __name__ == '__main__':
    unittest.main()