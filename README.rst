=======
Neo4jDB
=======

Implements the Python DB API 2.0 for Neo4j, compatible with python 2.6, 2.7,
3.2 and 3.3 and Neo4j >= 2.0.0

http://legacy.python.org/dev/peps/pep-0249/

.. image:: https://travis-ci.org/jakewins/neo4jdb-python.svg?branch=master
   :target: https://travis-ci.org/jakewins/neo4jdb-python


Installing
----------

::

    pip install neo4jdb

Minimum viable snippet
----------------------

::

    import neo4j

    connection = neo4j.connect("http://localhost:7474")

    cursor = connection.cursor()
    for name, age in cursor.execute("MATCH (n:User) RETURN n.name, n.age"):
        print name, age

Usage
-----

The library generally adheres to the DB API, please refer to the documentation
for the DB API for detailed usage.

::

    # Write statements
    cursor.execute("CREATE (n:User {name:'Stevie Brook'}")
    connection.commit() # Or connection.rollback()

    # With positional parameters
    cursor.execute("CREATE (n:User {name:{0}})", "Bob")
    # or
    l = ['Bob']
    cursor.execute("CREATE (n:User {name:{0}})", *l)

    # With named parameters
    cursor.execute("CREATE (n:User {name:{name}})", name="Bob")
    # or
    d = {'name': 'Bob'}
    cursor.execute("CREATE (n:User {name:{name}})", **d)
    # or
    d = {'node': {'name': 'Bob'}}
    cursor.execute("CREATE (n:User {node})", **d)


If you ask Cypher to return Nodes or Relationships, these are represented as Node and Relationship types, which
are `dict` objects with additional metadata for id, labels, type, end_id and start_id.

::

    # Retrieve and access a node
    for node, in cursor.execute("MATCH (n) RETURN n"):
        print node.id, node.labels
        print node['a_property']

    # Retrieve and access a relationship
    for rel, in cursor.execute("MATCH ()-[r]->() RETURN r"):
        print rel.id, rel.type, rel.start_id, rel.end_id
        print rel['a_property']


Using the context manager. Any exception in the context will result in the exception being thrown and the transaction to be rolled back.

::

    from neo4j.contextmanager import Neo4jDBConnectionManager
    manager = contextmanager.Neo4jDBConnectionManager('http://localhost:7474')

    with manager.read as r:  # r is just a cursor
        for name, age in r.execute("MATCH (n:User) RETURN n.name, n.age"):
            print name, age
    # When leaving read context the transaction will be rolled back

    with manager.write as w:
        w.execute("CREATE (n:User {name:{name}})", name="Bob")
    # When leaving write context the transaction will be committed

    # When using transaction a new connection will be created
    with manager.transaction as t:
        t.execute("CREATE (n:User {name:{name}})", name="Bob")
    # When leaving transaction context the transaction will be
    # committed and the connection will be closed

    # Rolling back or commit in contexts
    with manager.transaction as t:
        t.execute("CREATE (n:User {name:{name}})", name="Bob")
        if something == True:
            t.connection.commit()  # This will commit the transaction
        else:
            t.connection.rollback()  # This will rollback the transaction


Building & Testing
------------------

Neo4jDB uses paver as its build system. To install paver::

    pip install paver

Then you can build Neo4jDB with::

    paver build

And install it with::

    paver install


Running tests requires a Neo4j server running on localhost. Paver can handle
this for you::

    paver start_server
    paver nosetests
    paver stop_server

    
Incompliance with the spec
--------------------------

**Parameters**

The library delegates to Neo4j for parameter substitution, which means it does
not use any of the standard parameter substitution types defined in the spec. 

Instead it uses curly brackets with named and/or positional parameters::

    {0} or {identifier}


**Type system**

Because the wire format for Neo4j is JSON, the library does not support the 
date or binary value types. This may change in the future as the wire format
for Neo4j evolves.

In a similar vein, because Neo4j is a schema-optional database, it may return
arbitrary types in each cell in the result table. As such, the description of the
result table always marks each column type as neo4j.MIXED.


License
-------

http://opensource.org/licenses/MIT
