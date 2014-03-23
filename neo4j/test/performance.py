
import neo4j
import time

def main():
    conn = neo4j.connect("http://localhost:7474")
    
    start = time.time()
    iterations = 1000
    for it in xrange(iterations):
        cursor = conn.cursor()
        for i in xrange(50):
            cursor.execute("CREATE (n:User)")
        conn.commit()
    delta = time.time() - start
    print('Tx/s: ' + str(iterations / delta))

if __name__ == "__main__":
    main()
