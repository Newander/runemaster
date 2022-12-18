"""
    Run specific service.
    Run CLI example:
        > python manage.py cli list
        > python manage.py backend run
"""
import sys

from backend.cli import list_pipelines, list_tasks, remove_pipeline, remove_task
from backend.server.run import run_server
from backend.src.models import TaskModel
from backend.utils import get_collection


def show_pipelines():
    print('Existed pipelines:')
    for p in list_pipelines():
        print(f' - Pipeline [{p["name"]}]: tasks: {p["tasks"]}')


def show_tasks(pipeline: str = None):
    print('Pipelines / tasks:')
    for p in list_pipelines(pipeline):
        print(f' - Pipeline [{p["name"]}]:')
        for t in list_tasks(p['key']):
            print(f'    - Task [{t["name"]}]: {t["task_type"]}')


def show_rm_pipeline(pipeline: str):
    try:
        record_to_remove = get_collection('pipeline').find({'name': pipeline}).next()
    except StopIteration:
        print('Wrong pipeline name to remove:', pipeline)
        return

    remove_pipeline(record_to_remove['_key'])
    print(f'Pipeline [{pipeline}] is removed')


def show_rm_task(pipeline: str, task: str):
    try:
        pipe_record = get_collection('pipeline').find({'name': pipeline}).next()
    except StopIteration:
        print('The chosen pipeline has wrong name:', pipeline)
        return

    task_col = get_collection('task')
    try:
        record_to_remove: TaskModel = task_col.find({'pipeline_key': pipe_record['_key'], 'name': task}).next()
    except StopIteration:
        print('Wrong task name to remove:', task)
        return

    remove_task(pipe_record['_key'], record_to_remove['_key'])
    print(f'Task [{pipeline}:{task}] is removed')


def show_add_pipeline():
    ...


def show_add_task():
    ...


# Special cli interface tree
commands_tree = {
    'help': 'CLI manager of Runemaster project',
    'commands': {
        'cli': {
            'commands': {
                'list': {
                    'commands': {
                        'pipelines': {
                            'help': 'cli list pipelines',
                            'function': show_pipelines
                        },
                        'tasks': {
                            'options': {'pipeline'},
                            'help': 'cli list tasks',
                            'function': show_tasks
                        }
                    },
                    'help': 'help cli list'
                },
                'get': {
                    'help': 'help cli get'

                },
                'add': {
                    'help': 'help cli add'

                },
                'rm': {
                    'commands': {
                        'pipeline': {
                            'options': {'pipeline'},
                            'help': 'cli rm pipeline',
                            'function': show_rm_pipeline
                        },
                        'task': {
                            'options': {'pipeline', 'task'},
                            'help': 'cli rm task',
                            'function': show_rm_task
                        }
                    },
                    'help': 'help cli rm'
                },
            },
            'help': 'help 2 level cli'
        },
        'backend': {
            'commands': {
                'run': {
                    'options': {'port'},
                    'help': 'backend run helper',
                    'function': run_server
                }
            },
            'help': 'help 2 level backend'
        }
    }
}


def dive_parsing(left_args: list[str], left_cmd_tree: dict):
    if not left_args:
        if 'commands' in left_cmd_tree:
            raise Exception('Not enough arguments')
        else:
            return left_cmd_tree

    current_arg, left_left_args = left_args[0], left_args[1:]
    commands = left_cmd_tree['commands']

    if current_arg not in commands:
        raise Exception('Unexpected arguments')

    return dive_parsing(left_left_args, commands[current_arg])


if __name__ == '__main__':
    try:
        start_reading = next(i for i, arg in enumerate(sys.argv) if arg.endswith('manage.py'))
    except StopIteration:
        start_reading = None
    try:
        stop_reading = next(i for i, arg in enumerate(sys.argv) if arg.startswith('--'))
    except StopIteration:
        stop_reading = None
    arguments_list = sys.argv[start_reading + 1:stop_reading]

    options = {}
    if stop_reading is not None:
        current_arg_name = None
        for kwarg_op in sys.argv[stop_reading:]:
            if current_arg_name is None:
                current_arg_name = kwarg_op.lstrip('-')
            else:
                options[current_arg_name] = kwarg_op
                current_arg_name = None

    if not arguments_list:
        # print help, print error that there are no any required arguments
        print(commands_tree['help'])
    else:
        command_description = dive_parsing(arguments_list, commands_tree)

        # print(command_description)
        # print(options)

        if set(options) & command_description.get('options', set()) != set(options):
            raise Exception('Incorrect options')

        command_description['function'](**options)
