from radical.entk import Pipeline, Stage, Task, AppManager
import pytest
from radical.entk.exceptions import *
import os

hostname = os.environ.get('RMQ_HOSTNAME','localhost')
port = int(os.environ.get('RMQ_PORT',5672))

if __name__ == '__main__':

    """
    **Purpose**: Run an EnTK application on localhost
    """

    p1 = Pipeline()
    p1.name = 'p1'

    s1 = Stage()
    s1.name = 's1'
    for x in range(10):
        t = Task()
        t.name = 't%s'%x
        t.executable = ['/bin/hostname']
        t.arguments = ['>','hostname_%s.txt'%x,'|', 'sleep', '30']
        t.cpu_reqs['processes'] = 1
        t.cpu_reqs['threads_per_process'] = 16
        # t.lfs = 10
        t.download_output_data = ['hostname_%s.txt > s1_t%s_hostname_%s.txt'%(x,x,x)]

        s1.add_tasks(t)

    p1.add_stages(s1)

    s2 = Stage()
    s2.name = 's2'
    for x in range(10):
        t = Task()
        t.executable = ['/bin/hostname']
        t.arguments = ['>','hostname_%s.txt'%x,'|', 'sleep', '30']
        t.cpu_reqs['processes'] = 1
        t.cpu_reqs['threads_per_process'] = 16
        t.download_output_data = ['hostname_%s.txt > s2_t%s_hostname_%s.txt'%(x,x,x)]
        t.tag = 't%s'%x

        s2.add_tasks(t)


    p1.add_stages(s2)

    # res_dict = {

    #         'resource': 'local.localhost',
    #         'walltime': 5,
    #         'cpus': 1,
    # }

    res_dict = {

           'resource': 'ncsa.bw_aprun',
           'walltime': 10,
           'cpus': 176,
           'project': 'gk4',
           'queue': 'high'
    }

    # res_dict = {
    #         'resource': 'xsede.comet',
    #         'walltime': 30,
    #         'cpus': 240,
    #         'project': 'unc100',
    #     }


    os.environ['RADICAL_PILOT_DBURL'] = 'mongodb://entk:entk123@ds227821.mlab.com:27821/entk_0_7_0_release'

    appman = AppManager(hostname=hostname, port=port)
    appman.resource_desc = res_dict 
    appman.workflow = [p1]
    appman.run()
