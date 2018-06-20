import pytest
from radical.entk.utils import get_session_profile, get_session_description, write_workflow
from pprint import pprint
from radical.entk.exceptions import *
import radical.utils as ru
import os
from radical.entk import Pipeline, Stage, Task
from glob import glob

def test_get_session_profile():

    sid = 're.session.vivek-HP-Pavilion-m6-Notebook-PC.vivek.017598.0002'
    curdir = os.path.dirname(os.path.abspath(__file__))
    src = '%s/sample_data/profiler' % curdir
    profile, acc, hostmap = get_session_profile(sid=sid, src=src)

    # ip_prof = ru.read_json('%s/expected_profile.json'%src)
    for item in profile:
        assert len(item) == 8
    assert isinstance(acc, float)
    assert isinstance(hostmap, dict)


def test_get_session_description():

    sid = 're.session.vivek-HP-Pavilion-m6-Notebook-PC.vivek.017598.0002'
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
        t2.copy_input_data = ['$Pipline_%s_Stage_%s_Task_%s/output.txt' % (p.name, s1.name, t1.name)]

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
    
    for f in glob('test/*'):
        os.remove(f)

    write_workflow(wf,'test')

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
