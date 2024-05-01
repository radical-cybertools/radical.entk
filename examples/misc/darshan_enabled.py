#!/usr/bin/env python3

import os

import radical.entk  as re
import radical.pilot as rp

from radical.entk.tools import (cache_darshan_env,
                                with_darshan,
                                enable_darshan,
                                get_provenance_graph)

RESOURCE_DESCRIPTION = {
    # https://radicalpilot.readthedocs.io/en/stable/supported/polaris.html
    'resource': 'anl.polaris',
    'project' : 'RECUP',
    'queue'   : 'debug',
    'cpus'    : 32,
    'walltime': 15
}
# RESOURCE_DESCRIPTION = {
#     # https://radicalpilot.readthedocs.io/en/stable/supported/frontier.html
#     'resource': 'ornl.frontier',
#     'project' : 'NNNMMM',
#     'queue'   : 'batch',
#     'cpus'    : 56,
#     'walltime': 15
# }

os.environ['RADICAL_LOG_LVL'] = 'DEBUG'
os.environ['RADICAL_REPORT']  = 'TRUE'

TASK_01_OUTPUT = 'output_01.dat'


# pylint: disable=anomalous-backslash-in-string
def get_stage_0():

    # hello-RP task
    task_00 = re.Task({
        'executable': 'radical-pilot-hello.sh',
        'arguments' : [10],
        'cpu_reqs'  : {'cpu_processes'  : 1,
                       'cpu_threads'    : 4,
                       'cpu_thread_type': rp.OpenMP}
    })

    # R/W data
    task_01   = re.Task({
        'executable'       : '/bin/sh',
        'arguments'        : ['-c', f'cat input.dat | wc > {TASK_01_OUTPUT}'],
        'upload_input_data': ['/etc/passwd > input.dat'],
        'copy_output_data' : [f'{TASK_01_OUTPUT} > $SHARED/{TASK_01_OUTPUT}']
    })

    stage_0 = re.Stage()
    # --- enable Darshan for task "task_01" only
    stage_0.add_tasks([task_00, enable_darshan(task_01)])
    return stage_0


# --- enable Darshan for the whole "stage_1" using decorator
@with_darshan
def get_stage_1():

    # R/W data and task depends on the task from the previous stage
    task_10 = re.Task({
        'executable'     : '/bin/sh',
        'arguments'      : ['-c',
                            f"sed -r 's/\s+//g' {TASK_01_OUTPUT} " +  # noqa: W605
                            '| grep -o . | sort | uniq -c > output_10.dat'],
        'copy_input_data': [f'$SHARED/{TASK_01_OUTPUT} > {TASK_01_OUTPUT}']
    })

    stage_1 = re.Stage()
    stage_1.add_tasks([task_10])
    return stage_1


def main():

    if 'polaris' in RESOURCE_DESCRIPTION['resource']:
        cache_darshan_env(
            darshan_runtime_root='/soft/perftools/darshan/darshan-3.4.4',
            modules=['darshan/3.4.4'])
    elif 'frontier' in RESOURCE_DESCRIPTION['resource']:
        cache_darshan_env(
            darshan_runtime_root='$OLCF_DARSHAN_RUNTIME_ROOT',
            modules=['darshan-runtime/3.4.0', 'darshan-util/3.4.0'])

    pipeline = re.Pipeline()
    pipeline.add_stages([get_stage_0(), get_stage_1()])
    workflow = [pipeline]

    amgr = re.AppManager()
    amgr.resource_desc = RESOURCE_DESCRIPTION
    amgr.workflow = workflow
    amgr.run()

    print(get_provenance_graph(pipelines=workflow,
                               output_file='entk_provenance.json'))


if __name__ == '__main__':
    main()

