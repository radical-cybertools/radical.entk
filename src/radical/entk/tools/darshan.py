
__copyright__ = 'Copyright 2024, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import glob

from typing import Optional, Dict, List, Union

import radical.utils as ru

from .. import Pipeline, Stage, Task

DARSHAN_LOG_DIR = '%(sandbox)s/darshan_logs'

_darshan_activation_cmds = None
_darshan_env             = None
_darshan_runtime_root    = None


# ------------------------------------------------------------------------------
#
def cache_darshan_env(darshan_runtime_root: Optional[str] = None,
                      modules: Optional[List[str]] = None,
                      env: Optional[Dict[str, str]] = None) -> None:

    global _darshan_runtime_root

    if _darshan_runtime_root is None:
        if (darshan_runtime_root and
                not darshan_runtime_root.startswith('$') and
                not darshan_runtime_root.startswith('/')):
            raise RuntimeError('Darshan root directory should be set with '
                               'either env variable or an absolute path '
                               f'(provided path: {darshan_runtime_root})')
        _darshan_runtime_root = darshan_runtime_root or '$DARSHAN_RUNTIME_ROOT'

    global _darshan_activation_cmds
    global _darshan_env

    if _darshan_activation_cmds is None:

        _darshan_activation_cmds = []
        for module in modules or []:
            _darshan_activation_cmds.append(f'module load {module}')
        for k, v in (env or {}).items():
            _darshan_activation_cmds.append(f'export {k.upper()}="{v}"')

        _darshan_env = ru.env_prep(pre_exec_cached=_darshan_activation_cmds)


# ------------------------------------------------------------------------------
# decorator to enable darshan for function that generates Pipeline, Stage, Task
def with_darshan(func,
                 darshan_runtime_root: Optional[str] = None,
                 modules: Optional[List[str]] = None,
                 env: Optional[Dict[str, str]] = None):
    def wrapper(*args, **kwargs):
        return enable_darshan(func(*args, **kwargs),
                              darshan_runtime_root=darshan_runtime_root,
                              modules=modules,
                              env=env)
    return wrapper


# ------------------------------------------------------------------------------
#
def enable_darshan(pst: Union[Pipeline, Stage, Task, List[Pipeline]],
                   darshan_runtime_root: Optional[str] = None,
                   modules: Optional[List[str]] = None,
                   env: Optional[Dict[str, str]] = None
                   ) -> Union[Pipeline, Stage, Task]:

    if not pst:
        raise ValueError('PST object is not provided')
    elif isinstance(pst, list):
        if not isinstance(pst[0], Pipeline):
            raise TypeError('List of Pipelines is not provided')
    elif not isinstance(pst, (Pipeline, Stage, Task)):
        raise TypeError('Non PST object provided')

    cache_darshan_env(darshan_runtime_root, modules, env)

    def _enable_darshan(src_task: Task):

        if not src_task.executable:
            return
        elif 'libdarshan.so' in src_task.executable:
            # Darshan is already enabled
            return

        darshan_log_dir = DARSHAN_LOG_DIR % {'sandbox': '${RP_TASK_SANDBOX}'}
        darshan_enable  = (f'LD_PRELOAD="{_darshan_runtime_root}'
                           '/lib/libdarshan.so" ')

        if src_task.cpu_reqs.cpu_processes == 1:
            darshan_enable += 'DARSHAN_ENABLE_NONMPI=1 '

        src_task.executable  = darshan_enable + src_task.executable
        src_task.pre_launch += [f'mkdir -p {darshan_log_dir}']
        src_task.pre_exec.extend(
            _darshan_activation_cmds +
            [f'export DARSHAN_LOG_DIR_PATH={darshan_log_dir}'])

    if isinstance(pst, list):
        for pipeline in pst:
            for stage in pipeline.stages:
                for task in stage.tasks:
                    _enable_darshan(task)

    elif isinstance(pst, Pipeline):
        for stage in pst.stages:
            for task in stage.tasks:
                _enable_darshan(task)

    elif isinstance(pst, Stage):
        for task in pst.tasks:
            _enable_darshan(task)

    elif isinstance(pst, Task):
        _enable_darshan(pst)

    return pst


# ------------------------------------------------------------------------------
#
def get_parsed_data(log: str, target_counters: Union[str, List[str]]) -> set:

    data = set()

    if not target_counters:
        return data

    grep_patterns = '-e ' + ' -e '.join(ru.as_list(target_counters))
    parser_cmd    = (f'darshan-parser {log} | grep {grep_patterns} | '
                     "awk '{print $5\":\"$6}'")
    out, err, ret = ru.sh_callout(parser_cmd, env=_darshan_env, shell=True)
    if ret:
        print(f'[ERROR] Darshan not able to parse "{log}": {err}')
    else:
        for line in out.split('\n'):
            if not line:
                continue
            value, file = line.split(':', 1)
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

    for log in glob.glob((DARSHAN_LOG_DIR % {'sandbox': task.path}) + '/*'):

        inputs.update(get_parsed_data(log, ['POSIX_BYTES_READ', 'STDIO_OPENS']))
        outputs.update(get_parsed_data(log, 'POSIX_BYTES_WRITTEN'))

    arguments = ' '.join(task.arguments)
    if '>' in arguments:
        output = arguments.split('>')[1].split(';')[0].strip()
        if not output.startswith('/') and not output.startswith('$'):
            output = task.path + '/' + output
        outputs.add(output)

    task.annotate(inputs=sorted(inputs), outputs=sorted(outputs))


# ------------------------------------------------------------------------------

