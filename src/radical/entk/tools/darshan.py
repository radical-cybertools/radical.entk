
__copyright__ = 'Copyright 2024, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import glob
import os
import re
import tempfile

from datetime import datetime
from typing import Optional, Dict, List, Union

import radical.utils as ru

from .. import Pipeline, Stage, Task

DARSHAN_LOGS_PATTERN = (r'^.+_id[0-9]+-(?P<pid>[0-9]+)_'
                        r'([0-9]{1,2})-([0-9]{1,2})-(?P<secs>[0-9]{1,5})-.+')

_darshan_activation_cmds = None
_darshan_env             = None
_darshan_runtime_root    = None
_darshan_log_path        = None
_darshan_cfg_file_local  = None
_start_datetime          = None

# use DARSHAN_CONFIG_PATH to set a path to the config
DARSHAN_CONFIG = """
# enable DXT modules if interested in fine-grained trace data
MOD_ENABLE      DXT_POSIX,DXT_MPIIO

# allocate 4096 file records for POSIX and MPI-IO modules
# (darshan only allocates 1024 per-module by default)
MAX_RECORDS     4096      POSIX,MPI-IO

# the '*' specifier can be used to apply settings for all modules
NAME_EXCLUDE    ^/lib,^/bin,^/usr   *
NAME_EXCLUDE    ^/tmp,^/etc,^/sys   *
NAME_EXCLUDE    .py$,.pyc$,.so$     *
NAME_EXCLUDE    /site-packages      *

# bump up Darshan's default memory usage to 8 MiB
MODMEM  8

# avoid generating logs for { git, ls, which, ld } binaries
APP_EXCLUDE     git,ls,which,ld
"""


# ------------------------------------------------------------------------------
#
def cache_darshan_env(darshan_runtime_root: Optional[str] = None,
                      modules: Optional[List[str]] = None,
                      env: Optional[Dict[str, str]] = None) -> None:
    """
    Cache Darshan environment and corresponding setup parameters.

    .. data:: darshan_runtime_root

        [type: `str` | default: `None`] Absolute path or environment variable
        that points to the Darshan root directory.

    .. data:: modules

        [type: `list` | default: `None`] List of required module names.
        These modules will be loaded to set up Darshan environment.

    .. data:: env

        [type: `dict` | default: `None`] Dictionary of required environment
        variables (variable names and values). These environment variables
        will be exported to set up Darshan environment.
    """

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
    global _darshan_log_path

    if _darshan_activation_cmds is None:

        _darshan_activation_cmds = []
        for module in modules or []:
            _darshan_activation_cmds.append(f'module load {module}')
        for k, v in (env or {}).items():
            _darshan_activation_cmds.append(f'export {k.upper()}="{v}"')

        _darshan_env = ru.env_prep(pre_exec_cached=_darshan_activation_cmds)

        logpath_cmd = 'darshan-config --log-path'
        out, err, ret = ru.sh_callout(logpath_cmd, env=_darshan_env, shell=True)

        if out is not None:
            out = out.strip()

        if ret or not out or 'DARSHAN_LOG_DIR_PATH' in out:
            print(f'[WARNING] Darshan log path not set: "{err}"')
            _darshan_log_path = '%(sandbox)s/darshan_logs'

        elif not ret and out:
            _darshan_log_path = out + '/%(year)s/%(month)s/%(day)s'

            global _start_datetime
            _start_datetime = datetime.now()

    global _darshan_cfg_file_local

    if _darshan_cfg_file_local is None:

        fd, _darshan_cfg_file_local = tempfile.mkstemp(prefix='rct.darshan.cfg')
        with os.fdopen(fd, 'w') as f:
            f.write(DARSHAN_CONFIG)


# ------------------------------------------------------------------------------
#
def with_darshan(func,
                 darshan_runtime_root: Optional[str] = None,
                 modules: Optional[List[str]] = None,
                 env: Optional[Dict[str, str]] = None):
    """
    Decorator to enable Darshan for a corresponding function, which generates
    either Pipeline (or list of Pipelines, a.k.a. workflow) or Stage or Task.
    """
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
    """
    Enables Darshan for provided Pipeline (or list of Pipelines,
    a.k.a. workflow) or Stage or Task.

    .. data:: pst

        [type: `Pipeline, Stage, Task, list[Pipeline]`] PST object,
        which will be configured to enable Darshan.

    .. data:: darshan_runtime_root, modules, env

        See function `cache_darshan_env`.
    """

    if not pst:
        raise ValueError('PST object is not provided')
    elif isinstance(pst, list):
        if not isinstance(pst[0], Pipeline):
            raise TypeError('List of Pipelines is not provided')
    elif not isinstance(pst, (Pipeline, Stage, Task)):
        raise TypeError('Non PST object provided')

    cache_darshan_env(darshan_runtime_root, modules, env)

    # --------------------------------------------------------------------------
    def _enable_darshan(src_task: Task):

        if not src_task.executable:
            return
        elif 'libdarshan.so' in src_task.executable:
            # Darshan is already enabled
            return

        src_task.executable  = (f'env LD_PRELOAD="{_darshan_runtime_root}'
                                f'/lib/libdarshan.so" {src_task.executable}')

        # prepare and move Darshan config file into task sandbox
        src_task.upload_input_data.append(f'{_darshan_cfg_file_local} > '
                                          '$SHARED/darshan.cfg')
        darshan_cfg_file = '$RP_PILOT_SANDBOX/darshan.cfg'
        src_task.pre_exec.extend(
            _darshan_activation_cmds +
            [f'export DARSHAN_CONFIG_PATH={darshan_cfg_file}'])

        if '%(sandbox)s' in _darshan_log_path:
            log_dir = _darshan_log_path % {'sandbox': '${RP_TASK_SANDBOX}'}
            src_task.pre_launch.append(f'mkdir -p {log_dir}')
            src_task.pre_exec.append(f'export DARSHAN_LOG_DIR_PATH={log_dir}')

        if src_task.cpu_reqs.cpu_processes == 1:
            src_task.pre_exec.append('export DARSHAN_ENABLE_NONMPI=1')
    # --------------------------------------------------------------------------

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

    """
    Extracts file names from parsed Darshan log according to target counters.

    .. data:: log

        [type: `str`] Full path of the Darshan log file.

    .. data:: target_counters

        [type: `str/list`] Target counter used to extract the name of the
        associated IO file. Examples: 'POSIX_BYTES_WRITTEN', 'POSIX_BYTES_READ',
        'STDIO_OPENS'.
    """

    data = set()

    if not target_counters:
        return data

    grep_patterns = '-e ' + ' -e '.join(ru.as_list(target_counters))
    parser_cmd    = (f'env LD_PRELOAD="{_darshan_runtime_root}'
                     f'/lib/libdarshan-util.so" darshan-parser {log} | '
                     f'grep {grep_patterns} | ' + "awk '{print $5\":\"$6}'")
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

    """
    Annotates a task with extracted IO files using Darshan logs.

    .. data:: task

        [type: `Task`] EnTK task to annotate.
    """
    if _darshan_env is None:
        return

    inputs  = set()
    outputs = set()

    log_files = []
    if '%(sandbox)s' in _darshan_log_path:
        for log in glob.glob(_darshan_log_path % {'sandbox': task.path} + '/*'):
            log_files.append(log)
    else:
        if _start_datetime is None:
            return

        stop_datetime = datetime.now()
        stop_secs     = ((stop_datetime.hour * 3600) +
                         (stop_datetime.minute * 60) + stop_datetime.second)
        start_secs    = ((_start_datetime.hour * 3600) +
                         (_start_datetime.minute * 60) + _start_datetime.second)

        username  = os.getenv('USER', os.getenv('USERNAME', ''))
        task_pids = set(task.metadata.get('exec_pid', []) +
                        task.metadata.get('rank_pid', []))
        for y in range(_start_datetime.year, stop_datetime.year + 1):
            for m in range(_start_datetime.month, stop_datetime.month + 1):
                for d in range(_start_datetime.day, stop_datetime.day + 1):
                    opts         = {'year': y, 'month': m, 'day': d}
                    logs_pattern = ((_darshan_log_path % opts) +
                                    f'/{username}_*')
                    for log in glob.glob(logs_pattern):
                        p = re.search(DARSHAN_LOGS_PATTERN, log)
                        if p:
                            if     (m == _start_datetime.month and
                                    d == _start_datetime.day and
                                    int(p.group('secs')) < start_secs):
                                continue
                            elif   (m == stop_datetime.month and
                                    d == stop_datetime.day and
                                    int(p.group('secs')) > stop_secs):
                                continue
                            elif int(p.group('pid')) not in task_pids:
                                continue
                            log_files.append(log)

    for log in log_files:
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

