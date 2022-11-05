from arango import ArangoClient

from backend.main import CSVQueryTask, DownloadTask, Pipeline, Task

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
    csv_query = CSVQueryTask(pipe.name, 'csv_query')
    csv_query.set_input_attributes(columns='id,connector_id,caption,state,credentials,create_date',
                                   query={'connector_id': ['select', 'distinct']})
    upload = Task(pipe.name, 'upload')

    pipe.add(download >> csv_query >> upload)
    pipe.dump()
