from abc import ABC

from arango import ArangoClient
from arango.collection import StandardCollection, VertexCollection
from arango.graph import Graph


class ArangoModuleMixin(ABC):

    def __init__(self, *args, **kwargs):
        self.record: dict | None = None  # Arango`s record dictionary

    @classmethod
    def from_arango(cls, collection: StandardCollection):
        return cls(**{k: v for k, v in ar_kwargs.items() if k not in ('_id', '_key')})  # todo: check

    def kwargs(self):
        """ Make a dictionary with all necessary for initialization arguments """
        raise NotImplementedError

    def key(self) -> str:
        """ A special method returns [_key] attribute for arango record """
        raise NotImplementedError

    def construct_record(self):
        return {'_key': self.key(), **self.kwargs()}


class Task(ArangoModuleMixin):
    def __init__(self, pipeline_key: str, name: str):
        super().__init__()
        self.pipeline_key = pipeline_key
        self.name = name

    def __rshift__(self, other):
        if not isinstance(other, Task):
            raise Exception('Unprocessable type!')

        return TaskGraph(self.pipeline_key, init_task=self) >> other

    def kwargs(self):
        return {'pipeline_key': self.pipeline_key, 'name': self.name}

    def key(self):
        return f'{self.pipeline_key}_{self.name}'

    def insert(self, ar_task: VertexCollection) -> dict:
        self.record = ar_task.insert({'_key': self.key(), **self.kwargs()})
        return self.record


class TaskGraph:
    def __init__(self, pipeline_key: str, init_task: Task = None):
        self.pipeline_key = pipeline_key
        self.task_ordered: list[list[Task]] = []
        self.steps = {0: []}

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
    def __init__(self, name: str, task_graph: TaskGraph = None):
        super().__init__()
        self.name = name
        self.task_graph = task_graph or TaskGraph(pipeline_key=self.name)

    # todo: step 1:
    def kwargs(self):
        return {'name': self.name,
                'tasks': [[task.record for task in task_group] for task_group in self.task_graph.task_ordered]}

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

