"""
    Run specific service.
    Run CLI example:
        > python manage.py cli list
        > python manage.py backend run
"""
import sys

from backend.cli import list_pipelines, list_tasks
from backend.server.run import run_server


def show_pipelines():
    print('Existed pipelines:')
    for p in list_pipelines():
        print(f' - Pipeline [{p["name"]}]: tasks: {p["tasks"]}')


def show_tasks(pipeline: str = None):
    print('Pipelines / tasks:')
    for p in list_pipelines():
        print(f' - Pipeline [{p["name"]}]:')
        for t in list_tasks(p['key']):
            print(f'    - Task [{t["name"]}]: {t["task_type"]}')


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
                            'help': 'backend run helper',
                            'function': show_tasks
                        }
                    },
                    'help': 'help cli list'
                },
                'get': {
                    'help': 'help cli get'

                },
                'rm': {
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
