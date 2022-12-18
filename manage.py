"""
    Run specific service.
    Run CLI example:
        > python manage.py cli list
        > python manage.py backend run
"""
import sys
from typing import Type

from backend.cli import list_pipelines, list_tasks, remove_pipeline, remove_task
from backend.server.run import run_server
from backend.src.main import Pipeline, Task
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


def add_another_task(new_pipeline: Pipeline):
    print('Choose the task type:')
    print('[', ', '.join(task.__name__ for task in Task.get_available_tasks()), ']')
    task_type = input('> ')
    print('Choose the task name:')
    task_name = input('> ')

    TaskCls: Type[Task] = next(task for task in Task.get_available_tasks() if task.__name__ == task_type)
    task_instance = TaskCls(new_pipeline.name, task_name)

    input_attributes = {}
    for attr in TaskCls.input_attributes:
        attr_name = attr["name"]
        if attr.get('optional') and \
                input(f'This field [{attr_name}] is optional, do you wish to skip it? [Y/N]') in ('Y',):
            continue

        match attr['type']:
            case 'input':
                attr_value = input(f'Type the value of [{attr_name}]: > ')
            case 'choose':
                print(f'Select one of the accessible values (by number) for the attribute [{attr_name}]:')
                attr_dict = {
                    i + 1: print(f'{i + 1}.', variant) and variant for i, variant in enumerate(attr['variants'])
                }
                attr_num = input('Your choice:')
                attr_value = attr_dict[attr_num]
            case _:
                raise NotImplementedError

        input_attributes[attr['id']] = attr_value
    task_instance.set_input_attributes(**input_attributes)
    return task_instance


def interactive_add_pipeline():
    print('Interactive pipeline creation interface runs')

    print('Enter the pipeline`s name')
    pipeline_name = input('> ')
    new_pipeline = Pipeline(pipeline_name)

    print(f'Do you wish to add a task to [{new_pipeline.name}] pipeline? [Y/N]')
    is_add_new_task = input('> ') in ('', 'Y')
    task_graph = None
    while is_add_new_task:
        another_task = add_another_task(new_pipeline)
        if task_graph is None:
            task_graph = another_task
        else:
            task_graph = task_graph >> another_task
        print(f'Do you wish to add a task to [{new_pipeline.name}] pipeline? [Y/N]')
        is_add_new_task = input('> ') in ('', 'Y')

    print('Do you wish to add custom variables to the pipeline?')
    is_add_variable = input('> ') in ('', 'Y')
    while is_add_variable:
        new_pipeline.variables[input('Key:')] = input('Value:')

    new_pipeline.add(task_graph)
    new_pipeline.dump()
    print(f'Pipeline [{pipeline_name}] is created')


def interactive_add_task():
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
                            'help': 'help cli list pipelines',
                            'function': show_pipelines
                        },
                        'tasks': {
                            'options': {'pipeline'},
                            'help': 'help cli list tasks',
                            'function': show_tasks
                        }
                    },
                    'help': 'help cli list'
                },
                'add': {
                    'commands': {
                        'pipeline': {
                            'help': 'cli add pipeline',
                            'function': interactive_add_pipeline
                        },
                        'task': {
                            'options': {'pipeline'},
                            'help': 'cli add task',
                            'function': interactive_add_task
                        }
                    },
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
