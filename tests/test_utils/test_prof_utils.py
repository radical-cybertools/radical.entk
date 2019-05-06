import os
import shutil

import radical.utils as ru

from radical.entk import Pipeline, Stage, Task, AppManager
from radical.entk.appman.wfprocessor import WFprocessor
from radical.entk.execman.rp import TaskManager
from radical.entk.utils import get_session_profile, get_session_description, write_session_description, write_workflow

MLAB = 'mongodb://entk:entk123@ds143511.mlab.com:43511/entk_0_7_4_release'
hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
port = int(os.environ.get('RMQ_PORT', 5672))

# pylint: disable=protected-access


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

    amgr._wfp = WFprocessor(sid=amgr.sid,
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

    # os.mkdir(amgr._sid)

    write_session_description(amgr)

    desc = ru.read_json('%s/radical.entk.%s.json' % (amgr._sid, amgr._sid))
    curdir = os.path.dirname(os.path.abspath(__file__))
    src = '%s/sample_data' % curdir
    assert desc == ru.read_json('%s/expected_desc_write_session.json' % src)


def test_get_session_description():

    sid = 're.session.vivek-HP-Pavilion-m6-Notebook-PC.vivek.017732.0002'
    curdir = os.path.dirname(os.path.abspath(__file__))
    src = '%s/sample_data/profiler' % curdir
    desc = get_session_description(sid=sid, src=src)

    assert desc == ru.read_json('%s/expected_desc_get_session.json' % src)


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
        t2.copy_input_data = [
            '$Pipeline_%s_Stage_%s_Task_%s/output.txt' % (p.name, s1.name, t1.name)]

        # Add the Task to the Stage
        s2.add_tasks(t2)
        s2_task_uids.append(t2.name)

    # Add Stage to the Pipeline
    p.add_stages(s2)

    return p


def test_write_workflow():

    try:
        wf = list()
        wf.append(generate_pipeline(1))
        wf.append(generate_pipeline(2))

        amgr = AppManager(hostname=hostname, port=port)
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
        assert len(data) == len(wf) + 1

        stack = data.pop(0)
        assert stack.keys() == ['stack']
        assert stack['stack'].keys() == ['sys', 'radical']
        assert stack['stack']['sys'].keys() == ["python", "pythonpath",
                                                "virtualenv"]
        assert set(stack['stack']['radical'].keys()) == set(['radical.saga', 'radical.pilot',
                                                    'radical.utils',
                                                    'radical.entk'])

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

        write_workflow(wf, 'test', workflow_fout='test_workflow')

        data = ru.read_json('test/test_workflow.json')
        assert len(data) == len(wf) + 1

        stack = data.pop(0)
        assert stack.keys() == ['stack']
        assert stack['stack'].keys() == ['sys', 'radical']
        assert stack['stack']['sys'].keys() == ["python", "pythonpath",
                                                "virtualenv"]
        assert set(stack['stack']['radical'].keys()) == set(['radical.saga', 'radical.pilot',
                                                    'radical.utils',
                                                    'radical.entk'])

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

        data = write_workflow(wf, 'test', workflow_fout='test_workflow',
                              fwrite=False)

        assert len(data) == len(wf) + 1

        stack = data.pop(0)
        assert stack.keys() == ['stack']
        assert stack['stack'].keys() == ['sys', 'radical']
        assert stack['stack']['sys'].keys() == ["python", "pythonpath",
                                                "virtualenv"]
        assert set(stack['stack']['radical'].keys()) == set(['radical.saga', 'radical.pilot',
                                                    'radical.entk',
                                                    'radical.utils'])

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

    except Exception as ex:
        raise ex

    finally:
        shutil.rmtree('test')
# pylint: enable=protected-access
