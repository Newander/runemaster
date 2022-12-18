from abc import ABC
from io import StringIO
from pathlib import Path

import pandas as pd
import paramiko
from arango import ArangoClient
from arango.collection import StandardCollection, VertexCollection
from arango.graph import Graph

TaskOrderedType = list['Task']


class ArangoModuleMixin(ABC):

    @classmethod
    def from_arango(cls, collection: StandardCollection, key: str):
        record = collection.get({'_key': key})
        instance = cls.from_arango_record(collection, record)
        return instance

    @classmethod
    def from_arango_record(cls, collection: StandardCollection, record: dict):
        raise NotImplementedError

    def __init__(self, *args, **kwargs):
        self.record: dict | None = None  # Arango`s record dictionary

    def kwargs(self):
        """ Make a dictionary with all necessary for initialization arguments """
        raise NotImplementedError

    def key(self) -> str:
        """ A special method returns [_key] attribute for arango record """
        raise NotImplementedError

    def construct_record(self):
        return {'_key': self.key(), **self.kwargs()}


class Task(ArangoModuleMixin):
    input_attributes: list[dict]

    @classmethod
    def from_arango_record(cls, collection: StandardCollection, record: dict):
        instance = cls(
            **{k: v for k, v in record.items() if not k.startswith('_') and k not in ('attributes', 'task_type')}
        )
        instance.attributes = record['attributes']
        return instance

    def __init__(self, pipeline_key: str, name: str):
        super().__init__()
        self.pipeline_key = pipeline_key
        self.name = name
        self.attributes = {}
        self.task_type = str(self.__class__.__name__)

    def __rshift__(self, other):
        if not isinstance(other, Task):
            raise Exception('Unprocessable type!')

        return TaskGraph(self.pipeline_key, init_task=self) >> other

    def __repr__(self):
        return f'<{self.__class__.__name__} #{self.key()}>'

    def kwargs(self):
        return {'pipeline_key': self.pipeline_key, 'name': self.name, 'attributes': self.attributes,
                'task_type': self.task_type}

    def key(self):
        return f'{self.pipeline_key}_{self.name}'

    def set_input_attributes(self, **input_attributes):
        for id_, value in input_attributes.items():
            found = False
            for source_attr in self.input_attributes:
                if source_attr['id'] == id_:
                    found = True
                    break

            if not found:
                raise ValueError(f'An input attribute with id {id_} was not find')

            self.attributes[id_] = {'input_attribute': source_attr, 'value': value}

    def insert(self, ar_task: VertexCollection) -> dict:
        self.record = ar_task.insert({'_key': self.key(), **self.kwargs()})
        return self.record


class DownloadTask(Task):
    """ Task to load file into system """

    input_attributes = [
        {'id': 'source', 'name': 'Source', 'type': 'choose', 'variants': ['Local File System']},
        {'id': 'path', 'name': 'Path', 'type': 'input'},
    ]

    def execute(self):
        if self.attributes['source']['value'] == 'Local File System':
            return open(self.attributes['path']['value']).read()
        else:
            raise ValueError('Unknown source')


class SSHUploadTask(Task):
    """ Task to upload file into a different file system through SSH """

    input_attributes = [
        {'id': 'ssh_host', 'name': 'Hostname', 'type': 'input'},
        {'id': 'ssh_user', 'name': 'User', 'type': 'input'},
        {'id': 'ssh_password', 'name': 'Password', 'type': 'input'},
        {'id': 'remote_path', 'name': 'Path On Remote Host', 'type': 'input'},
    ]

    def execute(self, local_dataset_path: str | Path):
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=self.attributes['ssh_host']['value'],
                           username=self.attributes['ssh_user']['value'],
                           password=self.attributes['ssh_password']['value'])
        ftp_client = ssh_client.open_sftp()
        ftp_client.put(local_dataset_path, str(Path(self.attributes['remote_path']['value']) / local_dataset_path.name))
        ftp_client.close()


class CSVQueryTask(Task):
    """ Querying CSV files in a specific language """

    input_attributes = [
        {'id': 'columns', 'name': 'Columns', 'type': 'input', 'optional': True},
        {'id': 'query', 'name': 'Query [specific language]', 'type': 'input'},
    ]

    def execute(self, csv_string: str):
        columns = self.attributes['columns']['value'].split(',')
        query_dict = self.attributes['query']['value']

        csv_pd = pd.read_csv(StringIO(csv_string), names=columns)

        csv_result = pd.DataFrame()
        for field, (operator, add) in query_dict.items():
            if operator == 'select':
                if add == 'distinct':
                    csv_result[field] = sorted(csv_pd[field].unique())
                else:
                    raise ValueError('Unknown addition command')
            else:
                raise ValueError('Unknown operator')

        return csv_result.to_csv(index=False)


class TaskGraph:

    @classmethod
    def from_arango(cls, collection: StandardCollection, pipeline_key: str, tasks: list[dict]):
        def find_task_class_by_name(task_cls_name: str):
            for sub_cls in Task.__subclasses__():
                if sub_cls.__name__ == task_cls_name:
                    return sub_cls
            return Task

        task_ordered = [
            find_task_class_by_name(task['task_type']).from_arango_record(collection, task) for task in tasks
        ]
        return cls(pipeline_key, task_ordered=task_ordered)

    def __init__(self, pipeline_key: str, init_task: Task = None, task_ordered: TaskOrderedType = None):
        self.pipeline_key = pipeline_key
        self.task_ordered: TaskOrderedType = task_ordered or []

        if init_task:
            if not isinstance(init_task, Task):
                raise Exception('Unprocessable type!')

            self.task_ordered.append(init_task)

    def __rshift__(self, other):
        if isinstance(other, Task):
            self.task_ordered.append(other)
            return self
        elif isinstance(other, TaskGraph):
            if not self and other:
                return other

            raise Exception('Unprocessable type!')
        else:
            raise Exception('Unprocessable type!')

    def __bool__(self):
        return bool(self.task_ordered)

    def upload(self, ar_graph: Graph):
        task = ar_graph.vertex_collection("task")
        edges = ar_graph.edge_collection("next")

        prev_task_ar = None
        for task_instance in self.task_ordered:
            now_task_ar = task_instance.insert(task)
            if prev_task_ar:
                edges.insert({"_from": prev_task_ar['_id'], "_to": now_task_ar['_id'],
                              'pipeline_key': self.pipeline_key})
            prev_task_ar = now_task_ar


class Pipeline(ArangoModuleMixin):
    @classmethod
    def from_arango_record(cls, collection: StandardCollection, record: dict):
        instance = cls(name=record['name'])
        task_graph = TaskGraph.from_arango(collection, pipeline_key=instance.key(), tasks=record['tasks'])

        instance.task_graph = task_graph
        instance.record = record

        return instance

    def __init__(self, name: str, task_graph: TaskGraph = None):
        super().__init__()
        self.name = name
        self.task_graph = task_graph or TaskGraph(pipeline_key=self.name)
        self.variables = {}

    def __repr__(self):
        return f'<Pipeline #{self.key()}>'

    def __iter__(self):
        yield from self.task_graph.task_ordered

    def kwargs(self):
        return {'name': self.name, 'variables': self.variables,
                'tasks': [task.construct_record() for task in self.task_graph.task_ordered]}

    def key(self):
        return f'{self.name}'

    def add(self, t: Task | TaskGraph):
        """ Добавляет одну таску или их цепочку / граф """
        self.task_graph = self.task_graph >> t

    def dump(self):
        """ Записывает пайплайн в Арангу """
        client = ArangoClient(hosts='http://localhost:8529')
        db = client.db('test', username='root')
        task_graph = db.graph("task_graph")
        pipeline = db.collection('pipeline')
        upd_pipe = pipeline.get({'_key': self.name})

        self.task_graph.upload(task_graph)  # todo: need to insert into self.kwargs in some way..?
        if upd_pipe and upd_pipe.result():
            pipeline.update(self.construct_record())
        else:
            pipeline.insert(self.construct_record())


class LocalStorage:
    def __init__(self, os_path: str = '/volumes/local'):
        self.path = Path(os_path)
        self.path.mkdir(exist_ok=True, parents=True)

    def local_dataset_path(self, pipeline_key: str, task_key: str, file_name: str):
        return self.path / pipeline_key / task_key / file_name

    def prepare_pipeline(self, pipeline_key: str):
        (self.path / pipeline_key).mkdir(exist_ok=True)

    def prepare_task(self, pipeline_key: str, task_key: str):
        (self.path / pipeline_key / task_key).mkdir(exist_ok=True)

    def save_dataset(self, pipeline_key: str, task_key: str, new_dataset: str, file_name: str):
        self.local_dataset_path(pipeline_key, task_key, file_name).write_text(new_dataset)

    def get_dataset(self, pipeline_key: str, task_key: str, file_name: str):
        return self.local_dataset_path(pipeline_key, task_key, file_name).read_text()


class LocalEngine:
    """ Running pipeline """

    def __init__(self):
        self.storage = LocalStorage()

    def run(self, pipeline: Pipeline):
        self.storage.prepare_pipeline(pipeline.key())
        for prev_task, task in zip([None] + list(pipeline), pipeline):
            self.storage.prepare_task(pipeline.key(), task.key())
            if isinstance(task, DownloadTask):
                new_dataset = task.execute()
                pipeline.variables['native_file_name'] = Path(task.attributes['path']['value']).name
                self.storage.save_dataset(pipeline.key(), task.key(), new_dataset,
                                          file_name=pipeline.variables['native_file_name'])
            elif isinstance(task, CSVQueryTask):
                dataset = self.storage.get_dataset(pipeline.key(), prev_task.key(),
                                                   file_name=pipeline.variables['native_file_name'])
                new_dataset = task.execute(dataset)
                self.storage.save_dataset(pipeline.key(), task.key(), new_dataset,
                                          file_name=pipeline.variables['native_file_name'])
            elif isinstance(task, SSHUploadTask):
                task.execute(
                    local_dataset_path=self.storage.local_dataset_path(
                        pipeline.key(), prev_task.key(), pipeline.variables['native_file_name']
                    )
                )
            else:  # todo: Tasks
                raise ValueError(f'We don\'t support {task}')
