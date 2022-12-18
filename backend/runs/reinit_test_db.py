import logging

from arango import ArangoClient

from backend.src.main import CSVQueryTask, DownloadTask, Pipeline, SSHUploadTask


logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    logging.info('Start')

    # Initialization of connection
    client = ArangoClient(hosts='http://localhost:8529')
    db = client.db('test', username='root')
    task_graph = db.graph("task_graph")
    pipeline = db.collection('pipeline')
    task = task_graph.vertex_collection("task")
    edges = task_graph.edge_collection("next")
    logging.info('All collections and db were called')

    # The script
    pipeline.truncate()
    task.truncate()
    edges.truncate()
    logging.info('All collections were cleared')

    pipe = Pipeline('test_pipeline')
    download = DownloadTask(pipe.name, 'download')
    download.set_input_attributes(source='Local File System', path='/home/newander/PIK_DWH_web_accounts.csv')
    csv_query = CSVQueryTask(pipe.name, 'csv_query')
    csv_query.set_input_attributes(columns='id,connector_id,caption,state,credentials,create_date',
                                   query={'connector_id': ['select', 'distinct']})
    upload = SSHUploadTask(pipe.name, 'upload')
    upload.set_input_attributes(
        ssh_host='10.97.100.218',
        ssh_user='gavrilovsa',
        ssh_password=None,
        remote_path='/home/gavrilovsa'
    )
    logging.info('All test tasks are declared')

    pipe.add(download >> csv_query >> upload)
    pipe.dump()
    logging.info('The data sent to ArangoDB')

    logging.info('Finish')
