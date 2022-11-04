from arango import ArangoClient
from arango.graph import Graph


class Task:
    def __init__(self, pipeline_key: str, name: str):
        self.pipeline_key = pipeline_key
        self.name = name

    def __rshift__(self, other):
        if not isinstance(other, Task):
            raise Exception('Unprocessable type!')

        return TaskGraph(self.pipeline_key, init_task=self) >> other


class TaskGraph:
    def __init__(self, pipeline_key: str, init_task: Task = None):
        self.pipeline_key = pipeline_key
        if not isinstance(init_task, Task):
            raise Exception('Unprocessable type!')

        self.task_ordered: list[list[Task]] = []
        self.steps = {0: []}

        if init_task:
            self.task_ordered.append([init_task])
            self.steps[0].append(init_task)

    def __rshift__(self, other):
        if not isinstance(other, Task):
            raise Exception('Unprocessable type!')

        self.task_ordered.append([other])
        self.steps[max(self.steps) + 1] = [other]

        return self

    def upload(self, ar_graph: Graph):
        task = ar_graph.vertex_collection("task")
        edges = ar_graph.edge_collection("next")

        for task_step in self.task_ordered:
            task.insert()   # todo:


class Pipeline:
    # todo: step 1:
    def __init__(self, name: str, task_graph: TaskGraph = None):
        self.name = name
        self.task_graph = task_graph or TaskGraph(pipeline_key=self.name)

    def add(self, t: Task | TaskGraph):
        """ Добавляет одну таску или их цепочку / граф """
        self.task_graph >> t

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
    # todo: Scenario - construct pipeline
    pipe = Pipeline('test_pipeline')
    download = Task(pipe.name, 'download')
    sql_query = Task(pipe.name, 'sql_query')
    upload = Task(pipe.name, 'upload')

    pipe.add(download >> sql_query >> upload)
    pipe.dump()

    # todo: Scenario - execute pipeline
