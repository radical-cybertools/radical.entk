
__copyright__ = 'Copyright 2024, The RADICAL-Cybertools Team'
__license__   = 'MIT'

from typing import Optional, Dict, List, Union

import radical.utils as ru

from .. import Pipeline

from .darshan import annotate_task_with_darshan


# ------------------------------------------------------------------------------
#
def get_provenance_graph(pipelines: Union[Pipeline, List[Pipeline]],
                         output_file: Optional[str] = None) -> Dict:
    """
    Using UIDs of all entities to build a workflow provenance graph.
    """

    graph = {}

    for pipeline in ru.as_list(pipelines):
        graph[pipeline.uid] = {}
        for stage in pipeline.stages:
            graph[pipeline.uid][stage.uid] = {}
            for task in stage.tasks:
                annotate_task_with_darshan(task)
                graph[pipeline.uid][stage.uid][task.uid] = \
                    task.annotations.as_dict()

    if output_file:
        if not output_file.endswith('.json'):
            output_file += '.json'
        ru.write_json(graph, output_file)

    return graph


# ------------------------------------------------------------------------------
#
def extract_provenance_graph(session_json: str,
                             output_file: Optional[str] = None) -> Dict:
    """
    Using session JSON file to build a workflow provenance graph.
    """

    session_entities = ru.read_json(session_json)

    if not session_entities.get('task'):
        raise ValueError('No task entities in provided session')

    graph = {}

    for task in session_entities['task']:
        task_uid, _, stage_uid, _, pipeline_uid, _ = task['name'].split(',')
        graph.\
            setdefault(pipeline_uid, {}).\
            setdefault(stage_uid, {}).\
            setdefault(task_uid,
                       task['description']['metadata'].get('data') or {})

    for pipeline_uid in graph:
        for stage_uid in graph[pipeline_uid]:
            graph[pipeline_uid][stage_uid].sort()

    if output_file:
        if not output_file.endswith('.json'):
            output_file += '.json'
        ru.write_json(graph, output_file)

    return graph


# ------------------------------------------------------------------------------

