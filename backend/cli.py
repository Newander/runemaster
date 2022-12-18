"""
    Я вижу тут такие команды:
        1. Получить список пайплайнов
        2. Создать новый пайплайн
        4. Добавить таск к пайплайну
        5. Изменить аргументы таски
        6. Удалить таск
        7. Листануть все таски пайплайна

"""
from typing import TypedDict

from backend.src.models import NextModel, TaskModel
from backend.utils import get_collection, get_db


class AggPipeline(TypedDict):
    key: str
    name: str
    variables: int
    tasks: int


def list_pipelines(pipeline: str = None) -> list[AggPipeline]:
    filter_ = ''
    if pipeline:
        filter_ = f"filter pipe.name == '{pipeline}'"

    return list(get_db().aql.execute(f'''
        for pipe in pipeline {filter_}
        return {{key: pipe._key, name: pipe.name, variables: count(pipe.variables), tasks: count(pipe.tasks)}}
    ''').batch())


def list_tasks(pipeline_key: str) -> list[TaskModel]:
    return list(
        get_db().aql.execute(
            ' for t in task filter t.pipeline_key == @pipe_key return t ',
            bind_vars={'pipe_key': pipeline_key}
        ).batch()
    )


def remove_pipeline(pipeline_key: str):
    get_collection('task').delete_match({'pipeline_key': pipeline_key})
    get_collection('next').delete_match({'pipeline_key': pipeline_key})
    get_collection('pipeline').delete({'_key': pipeline_key})


def remove_task(pipeline_key: str, task_key: str):
    task_col = get_collection('task')
    next_col = get_collection('next')
    # todo: this will work only for one output
    next_edge_id = f'task/{task_key}'
    all_nexts: list[NextModel] = list(next_col.find({'pipeline_key': pipeline_key}).batch())

    if len(all_nexts) == 1:
        next_col.delete_match({'pipeline_key': pipeline_key})
    else:
        try:
            next_from: list[NextModel] = list(next_col.find({'_from': next_edge_id}).batch())
        except StopIteration:
            next_from = []
        try:
            next_to: list[NextModel] = list(next_col.find({'_to': next_edge_id}).batch())
        except StopIteration:
            next_to = []

        if not next_to and next_from:
            next_col.delete_many([{'_id': r['_id']} for r in next_from])
        if next_to and not next_from:
            next_col.delete_many([{'_id': r['_id']} for r in next_to])
        elif len(next_from) == 1:
            next_col.update_match(
                filters={'_to': next_edge_id},
                body={'_to': next_from[0]['_to']}
            )
            next_col.delete({'_id': next_from[0]['_id']})
        else:
            raise NotImplementedError

    task_col.delete({'_key': task_key})
