from radical.entk import Pipeline, Stage, Task, AppManager
import pytest
from radical.entk.exceptions import *
import os
from glob import glob

hostname = os.environ.get('RMQ_HOSTNAME','localhost')
port = int(os.environ.get('RMQ_PORT',5672))
MLAB = 'mongodb://entk:entk123@ds143511.mlab.com:43511/entk_0_7_4_release'


def test_rp_da_scheduler_bw():

    """
    **Purpose**: Run an EnTK application on localhost
    """

    p1 = Pipeline()
    p1.name = 'p1'

    n = 10

    s1 = Stage()
    s1.name = 's1'
    for x in range(n):
        t = Task()
        t.name = 't%s'%x
        t.executable = '/bin/hostname'
        t.arguments = ['>','hostname.txt']
        t.cpu_reqs['processes'] = 1
        t.cpu_reqs['threads_per_process'] = 16
        t.cpu_reqs['thread_type'] = ''
        t.cpu_reqs['process_type'] = ''
        t.lfs_per_process = 10
        t.download_output_data = ['hostname.txt > s1_t%s_hostname.txt'%(x)]

        s1.add_tasks(t)

    p1.add_stages(s1)

    s2 = Stage()
    s2.name = 's2'
    for x in range(n):
        t = Task()
        t.executable = '/bin/hostname'
        t.arguments = ['>','hostname.txt']
        t.cpu_reqs['processes'] = 1
        t.cpu_reqs['threads_per_process'] = 16
        t.cpu_reqs['thread_type'] = ''
        t.cpu_reqs['process_type'] = ''
        t.download_output_data = ['hostname.txt > s2_t%s_hostname.txt'%(x)]
        t.tag = 't%s'%x

        s2.add_tasks(t)


    p1.add_stages(s2)

    res_dict = {
                'resource'      : 'ncsa.bw_aprun',
                'walltime'      : 10,
                'cpus'          : 128,
                'project'       : 'gk4',
                'queue'         : 'high'
            }

    os.environ['RADICAL_PILOT_DBURL'] = MLAB

    appman = AppManager(hostname=hostname, port=port)
    appman.resource_desc = res_dict
    appman.workflow = [p1]
    appman.run()

    for i in range(n):
        assert open('s1_t%s_hostname.txt'%i,'r').readline().strip() == open('s2_t%s_hostname.txt'%i,'r').readline().strip()


    txts = glob('%s/*.txt' % os.getcwd())
    for f in txts:
        os.remove(f)
