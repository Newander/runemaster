from typing import TypedDict

TaskOrderedType = list['Task']


class ArangoReturnDict(TypedDict):
    """ The commonest attributes of any Arango record """
    _id: str
    _key: str


class EdgeModel(ArangoReturnDict):
    _rev: str
    _from: str
    _to: str


class TaskModel(ArangoReturnDict):
    pipeline_key: str
    name: str
    attributes: dict
    task_type: str


class NextModel(EdgeModel):
    pipeline_key: str
