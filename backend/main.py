from abc import ABC

from arango import ArangoClient
from arango.graph import Graph


class ArangoModuleMixin(ABC):
    @classmethod
    def from_arango(cls, ar_kwargs: dict):
        return cls(**{k: v for k, v in ar_kwargs.items() if k not in ('_id', '_key')})  # todo: check

    def kwargs(self):
        """ Make a dictionary with all necessary for initialization arguments """
        raise NotImplementedError

    def key(self):
        """ A special method returns [_key] attribute for arango record """
        raise NotImplementedError


class Task(ArangoModuleMixin):
    def __init__(self, pipeline_key: str, name: str):
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
        if isinstance(other, (Task)):
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
                new_task = task.insert({'_key': task_instance.key(), **task_instance.kwargs()})
                now_task_ar.append(new_task)

            for prev_ar_task in prev_tasks_ar:
                for now_ar_task in now_task_ar:
                    edges.insert({"_from": prev_ar_task['_id'], "_to": now_ar_task['_id']})

            prev_tasks_ar = now_task_ar


class Pipeline:
    # todo: step 1:
    def __init__(self, name: str, task_graph: TaskGraph = None):
        self.name = name
        self.task_graph = task_graph or TaskGraph(pipeline_key=self.name)

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

        self.task_graph.upload(task_graph)
        if upd_pipe and upd_pipe.result():
            pipeline.update({'_key': self.name, 'tasks': ...})
        else:
            pipeline.insert({'_key': self.name, 'tasks': ...})


if __name__ == '__main__':
    client = ArangoClient(hosts='http://localhost:8529')
    db = client.db('test', username='root')
    task_graph = db.graph("task_graph")
    pipeline = db.collection('pipeline')
    task = task_graph.vertex_collection("task")
    edges = task_graph.edge_collection("next")

    pipeline.truncate()
    task.truncate()
    edges.truncate()

    # todo: Scenario - construct pipeline
    pipe = Pipeline('test_pipeline')
    download = Task(pipe.name, 'download')
    sql_query = Task(pipe.name, 'sql_query')
    upload = Task(pipe.name, 'upload')

    pipe.add(download >> sql_query >> upload)
    pipe.dump()

    # todo: Scenario - execute pipeline
