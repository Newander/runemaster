from arango import ArangoClient

from backend.src.main import LocalEngine, Pipeline

if __name__ == '__main__':
    # Initialization of connection
    client = ArangoClient(hosts='http://localhost:8529')
    db = client.db('test', username='root')
    task_graph = db.graph("task_graph")
    pipeline = db.collection('pipeline')
    task = task_graph.vertex_collection("task")
    edges = task_graph.edge_collection("next")

    # The script
    pipe = Pipeline.from_arango(pipeline, 'test_pipeline')
    engine = LocalEngine()
    engine.run(pipe)
    print(pipe)
