from arango import ArangoClient
from arango.collection import StandardCollection
from arango.database import StandardDatabase


def get_db() -> StandardDatabase:
    """ Add client invocation to collections get """
    client = ArangoClient(hosts="http://localhost:8529")
    return client.db("test", username="root")


def get_collection(collection_name: str) -> StandardCollection:
    """ Add client invocation to collections get """
    client = ArangoClient(hosts="http://localhost:8529")
    db = client.db("test", username="root")

    return db.collection(collection_name)
