#!/usr/bin/env python

from radical.entk import Pipeline, Stage, Task, AppManager
from radical.entk.exceptions import *
import os
import sys
import argparse

hostname = os.environ.get('RMQ_HOSTNAME','localhost')
port = int(os.environ.get('RMQ_PORT',5672))


def get_pipeline(shared_fs=False, size=1):

    p = Pipeline()
    p.name = 'p'

    n = 4

    s1 = Stage()
    s1.name = 's1'
    for x in range(n):
        t = Task()
        t.name = 't%s'%x

        # dd if=/dev/random bs=<byte size of a chunk> count=<number of chunks> of=<output file name>

        t.executable = 'dd'

        if not shared_fs:
            t.arguments = ['if=/dev/urandom','bs=%sM'%size, 'count=1', 'of=$NODE_LFS_PATH/s1_t%s.txt'%x]
        else:
            t.arguments = ['if=/dev/urandom','bs=%sM'%size, 'count=1', 'of=/home/vivek91/s1_t%s.txt'%x]

        t.cpu_reqs['processes'] = 1
        t.cpu_reqs['threads_per_process'] = 24
        t.cpu_reqs['thread_type'] = ''
        t.cpu_reqs['process_type'] = ''
        t.lfs_per_process = 1024

        s1.add_tasks(t)

    p.add_stages(s1)

    s2 = Stage()
    s2.name = 's2'
    for x in range(n):
        t = Task()
        t.executable = 'dd'

        if not shared_fs:
            t.arguments = ['if=$NODE_LFS_PATH/s1_t%s.txt'%x,'bs=%sM'%size, 'count=1', 'of=$NODE_LFS_PATH/s2_t%s.txt'%x]
        else:
            t.arguments = ['if=/home/vivek91/s1_t%s.txt'%x,'bs=%sM'%size, 'count=1', 'of=/home/vivek91/s2_t%s.txt'%x]

        t.cpu_reqs['processes'] = 1
        t.cpu_reqs['threads_per_process'] = 24
        t.cpu_reqs['thread_type'] = ''
        t.cpu_reqs['process_type'] = ''
        t.tag = 't%s'%x

        s2.add_tasks(t)


    p.add_stages(s2)

    return p



if __name__ == '__main__':

    args = argparse.ArgumentParser()
    args.add_argument('sharedfs')
    args.add_argument('size')

    args = args.parse_args()
    if args.sharedfs == 'shared':
        shared_fs = True
    else:
        shared_fs = False
    size = args.size

    print('SharedFS: ', shared_fs, size)

    os.environ['RADICAL_PILOT_DBURL'] = 'mongodb://entk:entk123@ds159631.mlab.com:59631/da-lfs-test'

    res_dict = {
                'resource'      : 'xsede.comet',
                'walltime'      : 30,
                'cpus'          : 120,
                'project'       : 'unc100'
                # 'project'       : 'gk4',
                # 'queue'         : 'high'
            }

    appman = AppManager(hostname=hostname, port=port)
    appman.resource_desc = res_dict

    p = get_pipeline(shared_fs=shared_fs, size=size)
    appman.workflow = [p]
    appman.run()
