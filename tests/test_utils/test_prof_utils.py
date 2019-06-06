#!/usr/bin/env python

import os
import shutil

import radical.utils as ru

from radical.entk import Pipeline, Stage, Task, AppManager
from radical.entk.appman.wfprocessor import WFprocessor
from radical.entk.execman.rp import TaskManager
from radical.entk.utils import get_session_profile, get_session_description
from radical.entk.utils import write_session_description, write_workflow

hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
port     = int(os.environ.get('RMQ_PORT', 5672))

pwd      = os.path.dirname(os.path.abspath(__file__))

# pylint: disable=protected-access


# ------------------------------------------------------------------------------
#
def test_get_session_profile():

    sid    = 're.session.vivek-HP-Pavilion-m6-Notebook-PC.vivek.017732.0002'
    src    = '%s/sample_data/profiler' % pwd

    profile, acc, hostmap = get_session_profile(sid=sid, src=src)

    for item in profile:
        assert len(item) == 8

    assert isinstance(acc,     float)
    assert isinstance(hostmap, dict)


# ------------------------------------------------------------------------------
#
def test_write_session_description():

    amgr = AppManager(hostname=hostname, port=port)
    amgr.resource_desc = {'resource': 'xsede.stampede',
                          'walltime': 60,
                          'cpus'    : 128,
                          'gpus'    : 64,
                          'project' : 'xyz',
                          'queue'   : 'high'}

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

    write_session_description(amgr)

    desc   = ru.read_json('%s/radical.entk.%s.json' % (amgr._sid, amgr._sid))
    src    = '%s/sample_data' % pwd

    assert desc == ru.read_json('%s/expected_desc_write_session.json' % src)


# ------------------------------------------------------------------------------
#
def test_get_session_description():

    sid  = 're.session.vivek-HP-Pavilion-m6-Notebook-PC.vivek.017732.0002'
    src  = '%s/sample_data/profiler' % pwd
    desc = get_session_description(sid=sid, src=src)

    assert desc == ru.read_json('%s/expected_desc_get_session.json' % src)


# ------------------------------------------------------------------------------
#
def generate_pipeline(nid):

    p = Pipeline()
    p.name = 'p%s' % nid

    s1 = Stage()
    s1.name = 's1'

    t1 = Task()
    t1.name       = 't2'
    t1.executable = ['/bin/echo']
    t1.arguments  = ['hello']

    # FIXME: the above task does not seem to produce output data?
    t1_data = '$Pipeline_%s_Stage_%s_Task_%s/output.txt' \
            % (p.name, s1.name, t1.name)

    s1.add_tasks(t1)
    p.add_stages(s1)

    s2 = Stage()
    s2.name = 's2'

    for cnt in range(10):

        # Create a Task object
        t2 = Task()
        t2.name            = 't%s' % (cnt + 1)
        t2.executable      = ['/bin/echo']
        t2.arguments       = ['world']
        t2.copy_input_data = [t1_data]

        s2.add_tasks(t2)

    p.add_stages(s2)

    return p


# ------------------------------------------------------------------------------
#
def test_write_workflow():

    # --------------------------------------------------------------------------
    def check_stack(stack):

        assert 'sys'           in stack
        assert 'radical'       in stack

        assert 'python'        in stack['sys']
        assert 'pythonpath'    in stack['sys']
        assert 'virtualenv'    in stack['sys']

        assert 'radical.utils' in stack['radical']
        assert 'radical.saga'  in stack['radical']
        assert 'radical.pilot' in stack['radical']
        assert 'radical.entk'  in stack['radical']


    # --------------------------------------------------------------------------
    def check_wf(wf, check):

        for p_idx,p in enumerate(wf['pipes']):

            assert p['uid']           == check[p_idx].uid
            assert p['name']          == check[p_idx].name
            assert p['state_history'] == check[p_idx].state_history

            for s_idx,s in enumerate(p['stages']):

                assert s['uid']           == check[p_idx].stages[s_idx].uid
                assert s['name']          == check[p_idx].stages[s_idx].name
                assert s['state_history'] == check[p_idx].stages[s_idx].state_history

                for t in check[p_idx].stages[s_idx].tasks:
                    assert t.to_dict() in s['tasks']

    # --------------------------------------------------------------------------
    try:
        wf = list()
        wf.append(generate_pipeline(1))
        wf.append(generate_pipeline(2))

        amgr = AppManager(hostname=hostname, port=port)
        amgr.workflow = wf
        amgr._wfp     = WFprocessor(sid=amgr._sid,
                                    workflow=amgr._workflow,
                                    pending_queue=amgr._pending_queue,
                                    completed_queue=amgr._completed_queue,
                                    mq_hostname=amgr._mq_hostname,
                                    port=amgr._port,
                                    resubmit_failed=amgr._resubmit_failed)
        amgr._wfp._initialize_workflow()
        check = amgr.workflow

        # ----------------------------------------------------------------------
        # check json output
        write_workflow(amgr.workflows, 'test')

        data = ru.read_json('test/entk_workflow.json')

        check_stack(data['stack'])
        check_wf(data['workflows'][0], check)

        assert len(data['workflows']) == 1

        shutil.rmtree('test')


        # ----------------------------------------------------------------------
        # check with  custom fname
        write_workflow(amgr.workflows, 'test', fname='wf.json')

        data = ru.read_json('test/wf.json')

        check_stack(data['stack'])
        check_wf(data['workflows'][0], check)

        assert len(data['workflows']) == 1

        shutil.rmtree('test')

        # ----------------------------------------------------------------------
        # check with data return
        data = write_workflow(amgr.workflows, 'test', fwrite=False)

        check_stack(data['stack'])
        check_wf(data['workflows'][0], check)

        assert len(data['workflows']) == 1

        # ----------------------------------------------------------------------
        # check with two workflows
        amgr.workflow = wf
        amgr._wfp     = WFprocessor(sid=amgr._sid,
                                    workflow=amgr._workflow,
                                    pending_queue=amgr._pending_queue,
                                    completed_queue=amgr._completed_queue,
                                    mq_hostname=amgr._mq_hostname,
                                    port=amgr._port,
                                    resubmit_failed=amgr._resubmit_failed)
        amgr._wfp._initialize_workflow()
        check = amgr.workflows

        data = write_workflow(amgr.workflows, 'test', fwrite=False)

        check_stack(data['stack'])
        check_wf(data['workflows'][0], check[0])
        check_wf(data['workflows'][1], check[0])

        assert len(data['workflows']) == 2

        shutil.rmtree('test')

    finally:
        try:    shutil.rmtree('test')
        except: pass


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

   test_get_session_profile()
   test_write_session_description()
   test_get_session_description()
   test_write_workflow()


# pylint: enable=protected-access
# ------------------------------------------------------------------------------

