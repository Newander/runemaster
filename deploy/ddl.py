from arango import ArangoClient

if __name__ == '__main__':
    client = ArangoClient(hosts="http://localhost:8529")
    sys_db = client.db("_system", username="root", password="passwd")
    sys_db.create_database("test")

    db = client.db("test", username="root")
    db.create_collection("pipeline")
