from arango import ArangoClient

from backend.main import DownloadTask, Pipeline, Task

if __name__ == '__main__':
    # Initialization of connection
    client = ArangoClient(hosts='http://localhost:8529')
    db = client.db('test', username='root')
    task_graph = db.graph("task_graph")
    pipeline = db.collection('pipeline')
    task = task_graph.vertex_collection("task")
    edges = task_graph.edge_collection("next")

    # The script
    pipeline.truncate()
    task.truncate()
    edges.truncate()

    pipe = Pipeline('test_pipeline')
    download = DownloadTask(pipe.name, 'download')
    download.set_input_attributes(source='Local File System', path='/home/newander/PIK_DWH_web_accounts.csv')
    sql_query = Task(pipe.name, 'sql_query')
    upload = Task(pipe.name, 'upload')

    pipe.add(download >> sql_query >> upload)
    pipe.dump()
