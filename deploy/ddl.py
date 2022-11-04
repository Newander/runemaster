from arango import ArangoClient

if __name__ == '__main__':
    client = ArangoClient(hosts="http://localhost:8529")
    sys_db = client.db("_system", username="root", password="passwd")

    if not sys_db.has_database('test'):
        sys_db.create_database("test")

    db = client.db("test", username="root")

    if not db.has_collection('pipeline'):
        db.create_collection("pipeline")

    task_graph = db.create_graph("task_graph")
    task = task_graph.create_vertex_collection("task")
    edges = task_graph.create_edge_definition(
        edge_collection="next",
        from_vertex_collections=["task"],
        to_vertex_collections=["task"]
    )
