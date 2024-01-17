
__copyright__ = 'Copyright 2024, The RADICAL-Cybertools Team'
__license__   = 'MIT'

from typing import Optional, Dict, List

import radical.utils as ru

from .. import Pipeline, Task


# ------------------------------------------------------------------------------
#
def enable_darshan(pipelines: List[Pipeline],
                   modules: Optional[str] = None) -> None:

    for pipeline in pipelines:
        for stage in pipeline.stages:
            for task in stage.tasks:

                darshan_enable = ('LD_PRELOAD="$DARSHAN_RUNTIME_ROOT'
                                  '/lib/libdarshan.so" ')

                is_mpi = True if task.cpu_reqs.cpu_processes > 1 else False
                if not is_mpi:
                    darshan_enable += 'DARSHAN_ENABLE_NONMPI=1 '

                task.executable = darshan_enable + task.executable

                task.pre_launch += ['mkdir -p $RP_TASK_SANDBOX/darshan_logs']

                for module in modules or []:
                    task.pre_exec.append('module load %s' % module)
                task.pre_exec.append(
                    'export DARSHAN_LOG_DIR_PATH=$RP_TASK_SANDBOX/darshan_logs')


# ------------------------------------------------------------------------------
#
def set_dataflow(task: Task) -> None:

    # TODO: go through Darshan logs to collect inputs and outputs
    task.annotate(inputs=[{}], outputs=[])

    task.annotations.inputs  = sorted(set(task.annotations.inputs))
    task.annotations.outputs = sorted(set(task.annotations.outputs))


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
                set_dataflow(task)

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

