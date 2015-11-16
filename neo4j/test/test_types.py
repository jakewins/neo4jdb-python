import unittest

import neo4j
from neo4j.strings import unicode_type


class TestTypes(unittest.TestCase):

    def setUp(self):
        self.conn = neo4j.connect("http://localhost:7474")
        self.conn.authorization('neo4j', 'testing')

    def test_nodes(self):
        # Given
        cursor = self.conn.cursor()
        cursor.execute("CREATE (n:Params {name:{0}})", "Bob")

        # When
        res = list(cursor.execute("MATCH (n:Params) RETURN n"))

        # Then
        self.assertEqual(len(res), 1)
        node = res[0][0]
        self.assertEqual(node.labels, ['Params'])
        self.assertEqual(node['name'], "Bob")

    def test_rels(self):
        # Given
        cursor = self.conn.cursor()
        cursor.execute("CREATE (n:Types)-[r:KNOWS]->() SET r.name={0}", "Bob")

        # When
        res = list(cursor.execute("MATCH (:Types)-[r]-() RETURN r"))

        # Then
        self.assertEqual(len(res), 1)
        rel = res[0][0]
        self.assertEqual(rel.type, 'KNOWS')
        self.assertEqual(rel['name'], "Bob")

    def test_nested_structures(self):
        # Given
        cursor = self.conn.cursor()
        cursor.execute("CREATE (n:Types {name:{0}})-[r:KNOWS]->() SET r.name={0}", "Bob")

        # When
        res = list(cursor.execute("MATCH (n:Types)-[r]-() RETURN [{rel:r, node:n}]"))

        # Then
        self.assertEqual(len(res), 1)
        
        complicated = res[0][0]
        rel = complicated[0]['rel']
        node = complicated[0]['node']

        self.assertEqual(rel.type, 'KNOWS')
        self.assertEqual(rel['name'], "Bob")

        self.assertTrue( isinstance(rel.start_id, unicode_type) )
        self.assertTrue( isinstance(rel.end_id, unicode_type) )
        self.assertTrue( isinstance(rel.id, unicode_type) )

        self.assertEqual(node.labels, ['Types'])
        self.assertEqual(node['name'], "Bob")
        self.assertTrue( isinstance(node.id, unicode_type) )



if __name__ == '__main__':
    unittest.main()