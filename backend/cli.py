"""
    Я вижу тут такие команды:
        1. Получить список пайплайнов
        2. Создать новый пайплайн
        3. Выбрать существующий пайплайн
        4. Добавить таск к пайплайну
        5. Изменить аргументы таски
        6. Удалить таск
        7. Листануть все таски пайплайна

"""
from typing import TypedDict

from backend.utils import get_db


class AggPipeline(TypedDict):
    key: str
    name: str
    variables: int
    tasks: int


class Task(TypedDict):
    pipeline_key: str
    name: str
    attributes: dict
    task_type: str


def list_pipelines(pipeline: str = None) -> list[AggPipeline]:
    filter_ = ''
    if pipeline:
        filter_ = f"filter pipe.name == '{pipeline}'"
    return list(get_db().aql.execute(f'''
        for pipe in pipeline {filter_}
        return {{key: pipe._key, name: pipe.name, variables: count(pipe.variables), tasks: count(pipe.tasks)}}
    ''').batch())


def list_tasks(pipeline_key: str) -> list[Task]:
    return list(
        get_db().aql.execute(
            ' for t in task filter t.pipeline_key == @pipe_key return t ',
            bind_vars={'pipe_key': pipeline_key}
        ).batch()
    )
