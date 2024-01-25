
__copyright__ = 'Copyright 2024, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import glob

from typing import Optional, Dict, List, Union

import radical.utils as ru

from .. import Pipeline, Task

_darshan_activation_cmds = []


# ------------------------------------------------------------------------------
#
def enable_darshan(pipelines: List[Pipeline],
                   darshan_runtime_root: Optional[str] = None,
                   modules: Optional[List[str]] = None) -> None:

    if darshan_runtime_root:
        if not darshan_runtime_root.startswith('/'):
            raise RuntimeError('Path for the darshan installation '
                               'should be an absolute path '
                               f'(provided path: {darshan_runtime_root})')
    else:
        darshan_runtime_root = '$DARSHAN_RUNTIME_ROOT'

    for module in modules or []:
        _darshan_activation_cmds.append(f'module load {module}')

    for pipeline in pipelines:
        for stage in pipeline.stages:
            for task in stage.tasks:

                darshan_log_dir = '${RP_TASK_SANDBOX}/${RP_TASK_ID}_darshan'
                darshan_enable  = (f'LD_PRELOAD="{darshan_runtime_root}'
                                   '/lib/libdarshan.so" ')

                is_mpi = True if task.cpu_reqs.cpu_processes > 1 else False
                if not is_mpi:
                    darshan_enable += 'DARSHAN_ENABLE_NONMPI=1 '

                task.executable  = darshan_enable + task.executable
                task.pre_launch += [f'mkdir -p {darshan_log_dir}']
                task.pre_exec.extend(
                    _darshan_activation_cmds +
                    [f'export DARSHAN_LOG_DIR_PATH={darshan_log_dir}'])


# ------------------------------------------------------------------------------
#
def parsed_data(log_file: str,
                target_counters: Union[str, List[str]]) -> set:
    cmd = ''
    if _darshan_activation_cmds:
        cmd += '; '.join(_darshan_activation_cmds) + '; '

    target_counters = '-e ' + ' -e '.join(ru.as_list(target_counters))
    cmd += (f'darshan-parser {log_file} | grep {target_counters} | '
            "awk '{print $6}'")

    data  = set()
    files = ru.sh_callout(cmd, shell=True)[0]
    if files:
        for file in files.split('\n'):
            if not file.startswith('/'):
                continue
            data.add(file)

    return data


# ------------------------------------------------------------------------------
#
def annotate_task_with_darshan(task: Task) -> None:
    inputs  = set()
    outputs = set()

    for log in glob.glob(f'{task.path}/{task.uid}_darshan/*'):

        inputs.update(parsed_data(log, ['POSIX_BYTES_READ', 'STDIO_OPENS']))
        outputs.update(parsed_data(log, 'POSIX_BYTES_WRITTEN'))

    arguments = ' '.join(task.arguments)
    if '>' in arguments:
        outputs.add(arguments.split('>')[1].split(';')[0].strip())

    task.annotate(inputs=sorted(inputs), outputs=sorted(outputs))


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

