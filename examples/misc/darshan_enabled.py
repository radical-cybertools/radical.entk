#!/usr/bin/env python3

import os

import radical.entk  as re
import radical.pilot as rp

from radical.entk.utils.provenance import enable_darshan, get_provenance_graph

RESOURCE_DESCRIPTION = {
    # https://radicalpilot.readthedocs.io/en/stable/supported/polaris.html
    'resource': 'anl.polaris',
    'project' : 'RECUP',
    'queue'   : 'debug',
    'cpus'    : 32,
    'walltime': 15
}

os.environ['RADICAL_LOG_LVL'] = 'DEBUG'
os.environ['RADICAL_REPORT']  = 'TRUE'


# pylint: disable=anomalous-backslash-in-string
def get_stages():

    # hello-RP task
    task_00 = re.Task({
        'executable': 'radical-pilot-hello.sh',
        'arguments' : [10],
        'cpu_reqs'  : {'cpu_processes'  : 1,
                       'cpu_threads'    : 4,
                       'cpu_thread_type': rp.OpenMP}
    })

    # R/W data
    output_01 = 'output_01.dat'
    task_01   = re.Task({
        'executable'       : '/bin/sh',
        'arguments'        : ['-c', f'cat input.dat | wc > {output_01}'],
        'upload_input_data': ['/etc/passwd > input.dat'],
        'copy_output_data' : [f'{output_01} > $SHARED/{output_01}']
    })

    stage_0 = re.Stage()
    stage_0.add_tasks([task_00, task_01])

    # R/W data and task depends on the task from the previous stage
    task_10 = re.Task({
        'executable'     : '/bin/sh',
        'arguments'      : ['-c',
                            f"sed -r 's/\s+//g' {output_01} " +     # noqa: W605
                            '| grep -o . | sort | uniq -c > output_10.dat'],
        'copy_input_data': [f'$SHARED/{output_01} > {output_01}']
    })

    stage_1 = re.Stage()
    stage_1.add_tasks([task_10])

    return [stage_0, stage_1]


def main():
    pipeline = re.Pipeline()
    pipeline.add_stages(get_stages())
    workflow = [pipeline]

    enable_darshan(pipelines=workflow,
                   modules=['e4s/22.08/PrgEnv-gnu',
                            'darshan-runtime',
                            'darshan-util'])

    amgr = re.AppManager()
    amgr.resource_desc = RESOURCE_DESCRIPTION
    amgr.workflow = workflow
    amgr.run()

    print(get_provenance_graph(pipelines=workflow,
                               output_file='entk_provenance.json'))


if __name__ == '__main__':
    main()

