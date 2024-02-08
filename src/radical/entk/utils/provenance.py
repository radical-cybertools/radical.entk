
__copyright__ = 'Copyright 2024, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import glob

from typing import Optional, Dict, List, Union

import radical.utils as ru

from .. import Pipeline, Task

_darshan_env = None


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

    global _darshan_env

    darshan_activation_cmds = []
    for module in modules or []:
        darshan_activation_cmds.append(f'module load {module}')
        _darshan_env = ru.env_prep(pre_exec_cached=darshan_activation_cmds)

    for pipeline in pipelines:
        for stage in pipeline.stages:
            for task in stage.tasks:

                darshan_log_dir = '${RP_TASK_SANDBOX}/${RP_TASK_ID}_darshan'
                darshan_enable  = (f'LD_PRELOAD="{darshan_runtime_root}'
                                   '/lib/libdarshan.so" ')

                if task.cpu_reqs.cpu_processes == 1:
                    darshan_enable += 'DARSHAN_ENABLE_NONMPI=1 '

                task.executable  = darshan_enable + task.executable
                task.pre_launch += [f'mkdir -p {darshan_log_dir}']
                task.pre_exec.extend(
                    darshan_activation_cmds +
                    [f'export DARSHAN_LOG_DIR_PATH={darshan_log_dir}'])


# ------------------------------------------------------------------------------
#
def get_parsed_data(log: str, target_counters: Union[str, List[str]]) -> set:

    data = set()

    grep_patterns = '-e ' + ' -e '.join(ru.as_list(target_counters))
    parser_cmd    = (f'darshan-parser {log} | grep {grep_patterns} | '
                     "awk '{print $5\":\"$6}'")
    out, err, ret = ru.sh_callout(parser_cmd, env=_darshan_env, shell=True)
    if ret:
        print(f'[ERROR] Darshan not able to parse "{log}": {err}')
    else:
        for o in out.split('\n'):
            if not o:
                continue
            value, file = o.split(':')
            try:               value = int(value)
            except ValueError: value = 0
            if value > 0 and file.startswith('/'):
                data.add(file)

    return data


# ------------------------------------------------------------------------------
#
def annotate_task_with_darshan(task: Task) -> None:

    inputs  = set()
    outputs = set()

    for log in glob.glob(f'{task.path}/{task.uid}_darshan/*'):

        inputs.update(get_parsed_data(log, ['POSIX_BYTES_READ', 'STDIO_OPENS']))
        outputs.update(get_parsed_data(log, 'POSIX_BYTES_WRITTEN'))

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

