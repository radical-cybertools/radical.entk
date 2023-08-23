
__copyright__ = 'Copyright 2023, The RADICAL-Cybertools Team'
__license__   = 'MIT'

from typing import Optional, Dict, List

import radical.utils as ru

from .. import Pipeline


# ------------------------------------------------------------------------------
#
def get_provenance_graph(pipelines: List[Pipeline],
                         output_file: Optional[str] = None) -> Dict:
    """
    Using UIDs of all entities to build a workflow provenance graph.
    """

    graph = {}

    pipelines = ru.as_list(pipelines)

    for pipeline in pipelines:
        graph[pipeline.uid] = {}

        for stage in pipelines.stages:
            graph[pipeline.uid][stage.uid] = {}

            for task in stage.tasks:
                g_task = graph[pipeline.uid][stage.uid].setdefault(task.uid, {})
                if task.annotations:
                    g_task.update(task.annotations.as_dict())

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

