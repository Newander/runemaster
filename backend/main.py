from arango import ArangoClient


class Task:
    def __init__(self, name: str):
        self.name = name

    def __rshift__(self, other):
        if not isinstance(other, Task):
            raise Exception('Unprocessable type!')

        return TaskGraph(self) >> other


class TaskGraph:
    def __init__(self, init_task: Task):
        if not isinstance(init_task, Task):
            raise Exception('Unprocessable type!')

        self.task_ordered = [init_task]
        self.steps = {0: [init_task]}

    def __rshift__(self, other):
        if not isinstance(other, Task):
            raise Exception('Unprocessable type!')

        self.task_ordered.append(other)
        self.steps[max(self.steps) + 1] = [other]


class Pipeline:
    # todo: step 1:
    def __init__(self, name: str):
        self.name = name
        self.task_graphs = []

    def add(self, t: Task| TaskGraph):
        """ Добавляет одну таску или их цепочку / граф """
        self.task_graphs.append(t)

    def dump(self):
        """ Записывает пайплайн в Арангу """
        client = ArangoClient(hosts="http://localhost:8529")
        db = client.db("test", username="root")
        pipeline = db.create_collection("pipeline")
        upd_pipe = pipeline.get({'name': self.name})

        result = upd_pipe.result()
        pipeline.update()   # todo: stop here



if __name__ == '__main__':
    # todo: Scenario - construct pipeline
    download = Task('download')
    sql_query = Task('sql_query')
    upload = Task('upload')

    pipe = Pipeline('test_pipeline')
    pipe.add(download >> sql_query >> upload)
    pipe.dump()

    # todo: Scenario - execute pipeline
