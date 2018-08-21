import pytest
from radical.entk.utils import get_session_profile, get_session_description, write_session_description, write_workflow
from pprint import pprint
from radical.entk.exceptions import *
import radical.utils as ru
import os
from radical.entk import Pipeline, Stage, Task, AppManager
from radical.entk.appman.wfprocessor import WFprocessor
from radical.entk.execman.rp import TaskManager
from glob import glob
import shutil

MLAB = 'mongodb://entk:entk123@ds143511.mlab.com:43511/entk_0_7_4_release'

def test_get_session_profile():

    sid = 're.session.vivek-HP-Pavilion-m6-Notebook-PC.vivek.017732.0002'
    curdir = os.path.dirname(os.path.abspath(__file__))
    src = '%s/sample_data/profiler' % curdir
    profile, acc, hostmap = get_session_profile(sid=sid, src=src)

    # ip_prof = ru.read_json('%s/expected_profile.json'%src)
    for item in profile:
        assert len(item) == 8
    assert isinstance(acc, float)
    assert isinstance(hostmap, dict)


def test_write_session_description():

    hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
    port = int(os.environ.get('RMQ_PORT', 5672))

    amgr = AppManager(hostname=hostname, port=port)
    amgr.resource_desc = {
        'resource': 'xsede.stampede',
        'walltime': 60,
        'cpus': 128,
        'gpus': 64,
        'project': 'xyz',
        'queue': 'high'
    }

    workflow = [generate_pipeline(1), generate_pipeline(2)]
    amgr.workflow = workflow

    amgr._wfp = WFprocessor(sid=amgr._sid,
                            workflow=amgr._workflow,
                            pending_queue=amgr._pending_queue,
                            completed_queue=amgr._completed_queue,
                            mq_hostname=amgr._mq_hostname,
                            port=amgr._port,
                            resubmit_failed=amgr._resubmit_failed)
    amgr._wfp._initialize_workflow()
    amgr._workflow = amgr._wfp.workflow

    amgr._task_manager = TaskManager(sid=amgr._sid,
                                     pending_queue=amgr._pending_queue,
                                     completed_queue=amgr._completed_queue,
                                     mq_hostname=amgr._mq_hostname,
                                     rmgr=amgr._resource_manager,
                                     port=amgr._port
                                     )

    os.mkdir(amgr._sid)

    write_session_description(amgr)

    desc = ru.read_json('%s/%s.json' % (amgr._sid, amgr._sid))

    assert desc == {'config': {},
                    'entities': {'appmanager': {'event_model': {},
                                                'state_model': None,
                                                'state_values': None},
                                 'pipeline': {'event_model': {},
                                              'state_model': {'CANCELED': 9,
                                                              'DESCRIBED': 1,
                                                              'DONE': 9,
                                                              'FAILED': 9,
                                                              'SCHEDULING': 2},
                                              'state_values': {'1': 'DESCRIBED',
                                                               '2': 'SCHEDULING',
                                                               '9': ['DONE',
                                                                     'CANCELED',
                                                                     'FAILED']}},
                                 'stage': {'event_model': {},
                                           'state_model': {'CANCELED': 9,
                                                           'DESCRIBED': 1,
                                                           'DONE': 9,
                                                           'FAILED': 9,
                                                           'SCHEDULED': 3,
                                                           'SCHEDULING': 2},
                                           'state_values': {'1': 'DESCRIBED',
                                                            '2': 'SCHEDULING',
                                                            '3': 'SCHEDULED',
                                                                 '9': ['FAILED',
                                                                       'CANCELED',
                                                                       'DONE']}},
                                 'task': {'event_model': {},
                                          'state_model': {'CANCELED': 9,
                                                          'DEQUEUED': 8,
                                                          'DEQUEUEING': 7,
                                                          'DESCRIBED': 1,
                                                          'DONE': 9,
                                                          'EXECUTED': 6,
                                                          'FAILED': 9,
                                                          'SCHEDULED': 3,
                                                          'SCHEDULING': 2,
                                                          'SUBMITTED': 5,
                                                          'SUBMITTING': 4},
                                          'state_values': {'1': 'DESCRIBED',
                                                           '2': 'SCHEDULING',
                                                           '3': 'SCHEDULED',
                                                           '4': 'SUBMITTING',
                                                           '5': 'SUBMITTED',
                                                           '6': 'EXECUTED',
                                                           '7': 'DEQUEUEING',
                                                           '8': 'DEQUEUED',
                                                           '9': ['DONE',
                                                                 'CANCELED',
                                                                 'FAILED']}}},
                    'tree': {'appmanager.0000': {'cfg': {},
                                                 'children': ['wfprocessor.0000',
                                                              'resource_manager.0000',
                                                              'task_manager.0000',
                                                              'pipeline.0000',
                                                              'pipeline.0001'],
                                                 'etype': 'appmanager',
                                                 'has': ['pipeline',
                                                         'wfprocessor',
                                                         'resource_manager',
                                                         'task_manager'],
                                                 'uid': 'appmanager.0000'},
                             'pipeline.0000': {'cfg': {},
                                               'children': ['stage.0000', 
                                                            'stage.0001'],
                                               'etype': 'pipeline',
                                               'has': ['stage'],
                                               'uid': 'pipeline.0000'},
                             'pipeline.0001': {'cfg': {},
                                               'children': ['stage.0002', 
                                                            'stage.0003'],
                                               'etype': 'pipeline',
                                               'has': ['stage'],
                                               'uid': 'pipeline.0001'},
                             'resource_manager.0000': {'cfg': {},
                                                       'children': [],
                                                       'etype': 'resource_manager',
                                                       'has': [],
                                                       'uid': 'resource_manager.0000'},
                             'stage.0000': {'cfg': {},
                                            'children': ['task.0000'],
                                            'etype': 'stage',
                                            'has': ['task'],
                                            'uid': 'stage.0000'},
                             'stage.0001': {'cfg': {},
                                            'children': ['task.0001',
                                                         'task.0002',
                                                         'task.0003',
                                                         'task.0004',
                                                         'task.0005',
                                                         'task.0006',
                                                         'task.0007',
                                                         'task.0008',
                                                         'task.0009',
                                                         'task.0010'],
                                            'etype': 'stage',
                                            'has': ['task'],
                                            'uid': 'stage.0001'},
                             'stage.0002': {'cfg': {},
                                            'children': ['task.0011'],
                                            'etype': 'stage',
                                            'has': ['task'],
                                            'uid': 'stage.0002'},
                             'stage.0003': {'cfg': {},
                                            'children': ['task.0012',
                                                         'task.0013',
                                                         'task.0014',
                                                         'task.0015',
                                                         'task.0016',
                                                         'task.0017',
                                                         'task.0018',
                                                         'task.0019',
                                                         'task.0020',
                                                         'task.0021'],
                                            'etype': 'stage',
                                            'has': ['task'],
                                            'uid': 'stage.0003'},
                             'task.0000': {'cfg': {},
                                           'children': [],
                                           'etype': 'task',
                                           'has': [],
                                           'uid': 'task.0000'},
                             'task.0001': {'cfg': {},
                                           'children': [],
                                           'etype': 'task',
                                           'has': [],
                                           'uid': 'task.0001'},
                             'task.0002': {'cfg': {},
                                           'children': [],
                                           'etype': 'task',
                                           'has': [],
                                           'uid': 'task.0002'},
                             'task.0003': {'cfg': {},
                                           'children': [],
                                           'etype': 'task',
                                           'has': [],
                                           'uid': 'task.0003'},
                             'task.0004': {'cfg': {},
                                           'children': [],
                                           'etype': 'task',
                                           'has': [],
                                           'uid': 'task.0004'},
                             'task.0005': {'cfg': {},
                                           'children': [],
                                           'etype': 'task',
                                           'has': [],
                                           'uid': 'task.0005'},
                             'task.0006': {'cfg': {},
                                           'children': [],
                                           'etype': 'task',
                                           'has': [],
                                           'uid': 'task.0006'},
                             'task.0007': {'cfg': {},
                                           'children': [],
                                           'etype': 'task',
                                           'has': [],
                                           'uid': 'task.0007'},
                             'task.0008': {'cfg': {},
                                           'children': [],
                                           'etype': 'task',
                                           'has': [],
                                           'uid': 'task.0008'},
                             'task.0009': {'cfg': {},
                                           'children': [],
                                           'etype': 'task',
                                           'has': [],
                                           'uid': 'task.0009'},
                             'task.0010': {'cfg': {},
                                           'children': [],
                                           'etype': 'task',
                                           'has': [],
                                           'uid': 'task.0010'},
                             'task.0011': {'cfg': {},
                                           'children': [],
                                           'etype': 'task',
                                           'has': [],
                                           'uid': 'task.0011'},
                             'task.0012': {'cfg': {},
                                           'children': [],
                                           'etype': 'task',
                                           'has': [],
                                           'uid': 'task.0012'},
                             'task.0013': {'cfg': {},
                                           'children': [],
                                           'etype': 'task',
                                           'has': [],
                                           'uid': 'task.0013'},
                             'task.0014': {'cfg': {},
                                           'children': [],
                                           'etype': 'task',
                                           'has': [],
                                           'uid': 'task.0014'},
                             'task.0015': {'cfg': {},
                                           'children': [],
                                           'etype': 'task',
                                           'has': [],
                                           'uid': 'task.0015'},
                             'task.0016': {'cfg': {},
                                           'children': [],
                                           'etype': 'task',
                                           'has': [],
                                           'uid': 'task.0016'},
                             'task.0017': {'cfg': {},
                                           'children': [],
                                           'etype': 'task',
                                           'has': [],
                                           'uid': 'task.0017'},
                             'task.0018': {'cfg': {},
                                           'children': [],
                                           'etype': 'task',
                                           'has': [],
                                           'uid': 'task.0018'},
                             'task.0019': {'cfg': {},
                                           'children': [],
                                           'etype': 'task',
                                           'has': [],
                                           'uid': 'task.0019'},
                             'task.0020': {'cfg': {},
                                           'children': [],
                                           'etype': 'task',
                                           'has': [],
                                           'uid': 'task.0020'},
                             'task.0021': {'cfg': {},
                                           'children': [],
                                           'etype': 'task',
                                           'has': [],
                                           'uid': 'task.0021'},
                             'task_manager.0000': {'cfg': {},
                                                   'children': [],
                                                   'etype': 'task_manager',
                                                   'has': [],
                                                   'uid': 'task_manager.0000'},
                             'wfprocessor.0000': {'cfg': {},
                                                  'children': [],
                                                  'etype': 'wfprocessor',
                                                  'has': [],
                                                  'uid': 'wfprocessor.0000'}}}

    shutil.rmtree(amgr._sid)


def test_get_session_description():

    sid = 're.session.vivek-HP-Pavilion-m6-Notebook-PC.vivek.017732.0002'
    curdir = os.path.dirname(os.path.abspath(__file__))
    src = '%s/sample_data/profiler' % curdir
    desc = get_session_description(sid=sid, src=src)

    assert desc == ru.read_json('%s/expected_desc.json' % src)


def generate_pipeline(nid):

    # Create a Pipeline object
    p = Pipeline()
    p.name = 'p%s' % nid

    # Create a Stage object
    s1 = Stage()
    s1.name = 's1'

    # Create a Task object which creates a file named 'output.txt' of size 1 MB
    t1 = Task()
    t1.name = 't2'
    t1.executable = ['/bin/echo']
    t1.arguments = ['hello']

    # Add the Task to the Stage
    s1.add_tasks(t1)

    # Add Stage to the Pipeline
    p.add_stages(s1)

    # Create another Stage object to hold character count tasks
    s2 = Stage()
    s2.name = 's2'
    s2_task_uids = []

    for cnt in range(10):

        # Create a Task object
        t2 = Task()
        t2.name = 't%s' % (cnt + 1)
        t2.executable = ['/bin/echo']
        t2.arguments = ['world']
        # Copy data from the task in the first stage to the current task's location
        t2.copy_input_data = ['$Pipeline_%s_Stage_%s_Task_%s/output.txt' % (p.name, s1.name, t1.name)]

        # Add the Task to the Stage
        s2.add_tasks(t2)
        s2_task_uids.append(t2.name)

    # Add Stage to the Pipeline
    p.add_stages(s2)

    return p


def test_write_workflow():

    wf = list()
    wf.append(generate_pipeline(1))
    wf.append(generate_pipeline(2))

    amgr = AppManager()
    amgr.workflow = wf
    amgr._wfp = WFprocessor(sid=amgr._sid,
                            workflow=amgr._workflow,
                            pending_queue=amgr._pending_queue,
                            completed_queue=amgr._completed_queue,
                            mq_hostname=amgr._mq_hostname,
                            port=amgr._port,
                            resubmit_failed=amgr._resubmit_failed)
    amgr._wfp._initialize_workflow()  
    wf = amgr._wfp.workflow

    write_workflow(wf, 'test')

    data = ru.read_json('test/entk_workflow.json')
    assert len(data) == len(wf)

    p_cnt = 0
    for p in data:
        assert p['uid'] == wf[p_cnt].uid
        assert p['name'] == wf[p_cnt].name
        assert p['state_history'] == wf[p_cnt].state_history
        s_cnt = 0
        for s in p['stages']:
            assert s['uid'] == wf[p_cnt].stages[s_cnt].uid
            assert s['name'] == wf[p_cnt].stages[s_cnt].name
            assert s['state_history'] == wf[p_cnt].stages[s_cnt].state_history
            for t in wf[p_cnt].stages[s_cnt].tasks:
                assert t.to_dict() in s['tasks']
            s_cnt += 1
        p_cnt += 1


    shutil.rmtree('test')