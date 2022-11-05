from abc import ABC
from pathlib import Path

from arango import ArangoClient
from arango.collection import StandardCollection, VertexCollection
from arango.graph import Graph

TaskOrderedType = list[list['Task']]


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
        return cls(**{k: v for k, v in record.items() if not k.startswith('_')})

    def __init__(self, pipeline_key: str, name: str):
        super().__init__()
        self.pipeline_key = pipeline_key
        self.name = name
        self.filled_attrs = {}

    def __rshift__(self, other):
        if not isinstance(other, Task):
            raise Exception('Unprocessable type!')

        return TaskGraph(self.pipeline_key, init_task=self) >> other

    def kwargs(self):
        return {'pipeline_key': self.pipeline_key, 'name': self.name}

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

            self.filled_attrs[id_] = {'input_attribute': source_attr, }#todo: working on here


    def insert(self, ar_task: VertexCollection) -> dict:
        self.record = ar_task.insert({'_key': self.key(), **self.kwargs()})
        return self.record


class DownloadTask(Task):
    """ Task to load file into system """

    input_attributes = [
        {'id': 'source', 'name': 'Source', 'type': 'choose', 'variants': ['Local File System']},
        {'id': 'path', 'name': 'Path', 'type': 'input'}
    ]


class TaskGraph:

    @classmethod
    def from_arango(cls, collection: StandardCollection, pipeline_key: str, tasks: list[list[dict]]):
        task_ordered = [[Task.from_arango_record(collection, task) for task in task_group] for task_group in tasks]
        return cls(pipeline_key, task_ordered=task_ordered)

    def __init__(self, pipeline_key: str, init_task: Task = None, task_ordered: TaskOrderedType = None):
        self.pipeline_key = pipeline_key
        self.task_ordered: TaskOrderedType = task_ordered or []
        self.steps = {i: task_group for i, task_group in enumerate(self.task_ordered)} or {0: []}

        if init_task:
            if not isinstance(init_task, Task):
                raise Exception('Unprocessable type!')

            self.task_ordered.append([init_task])
            self.steps[0].append(init_task)

    def __rshift__(self, other):
        if isinstance(other, Task):
            self.task_ordered.append([other])
            self.steps[max(self.steps) + 1] = [other]
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

        prev_tasks_ar = []
        for task_step in self.task_ordered:
            now_task_ar = []
            for task_instance in task_step:
                ar_task = task_instance.insert(task)
                now_task_ar.append(ar_task)

            for prev_ar_task in prev_tasks_ar:
                for now_ar_task in now_task_ar:
                    edges.insert({"_from": prev_ar_task['_id'], "_to": now_ar_task['_id']})

            prev_tasks_ar = now_task_ar


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

    def __repr__(self):
        return f'<Pipeline #{self.key()}>'

    def __iter__(self):
        yield from self.task_graph.task_ordered

    def kwargs(self):
        return {'name': self.name,
                'tasks': [[task.construct_record() for task in task_group] for task_group in
                          self.task_graph.task_ordered]}

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

    def prepare_pipeline(self, pipeline_key: str):
        (self.path / pipeline_key).mkdir(exist_ok=True)

    def prepare_task(self, pipeline_key: str, task_key: str):
        (self.path / pipeline_key / task_key).mkdir(exist_ok=True)


class LocalEngine:
    """ Running pipeline """

    def __init__(self):
        self.storage = LocalStorage()

    def run(self, pipeline: Pipeline):
        self.storage.prepare_pipeline(pipeline.key())
        for task_group in pipeline:
            for task in task_group:
                self.storage.prepare_task(pipeline.key(), task.key())
                if isinstance(task, DownloadTask):  # todo: Tasks
                    ...
                else:
                    raise ValueError(f'We don\'t support {task}')
