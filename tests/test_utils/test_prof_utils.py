#!/usr/bin/env python

import os
import shutil

import radical.utils as ru

from radical.entk                    import Pipeline, Stage, Task, AppManager
from radical.entk.appman.wfprocessor import WFprocessor
from radical.entk.execman.rp         import TaskManager
from radical.entk.utils              import get_session_profile
from radical.entk.utils              import get_session_description
from radical.entk.utils              import write_session_description
from radical.entk.utils              import write_workflows

hostname =     os.environ.get('RMQ_HOSTNAME', 'localhost')
port     = int(os.environ.get('RMQ_PORT', 5672))
username = os.environ.get('RMQ_USERNAME')
password = os.environ.get('RMQ_PASSWORD')
pwd      = os.path.dirname(os.path.abspath(__file__))


# pylint: disable=protected-access


# ------------------------------------------------------------------------------
#
def test_get_session_profile():

    src = '%s/sample_data/profiler' % pwd
    sid = 're.session.host.user.012345.1234'

    profile, acc, hostmap = get_session_profile(sid=sid, src=src)

    for item in profile:
        assert len(item) == 8

    assert isinstance(acc,     float)
    assert isinstance(hostmap, dict)


# ------------------------------------------------------------------------------
#
def test_write_session_description():

    amgr = AppManager(hostname=hostname, port=port, username=username,
            password=password)
    amgr.resource_desc = {'resource' : 'xsede.stampede',
                          'walltime' : 59,
                          'cpus'     : 128,
                          'gpus'     : 64,
                          'project'  : 'xyz',
                          'queue'    : 'high'}

    workflow      = [generate_pipeline(1), generate_pipeline(2)]
    amgr.workflow = workflow

    amgr._wfp = WFprocessor(sid=amgr.sid,
                            workflow=amgr._workflow,
                            pending_queue=amgr._pending_queue,
                            completed_queue=amgr._completed_queue,
                            resubmit_failed=amgr._resubmit_failed,
                            rmq_conn_params=amgr._rmq_conn_params)
    amgr._workflow = amgr._wfp.workflow

    amgr._task_manager = TaskManager(sid=amgr._sid,
                                     pending_queue=amgr._pending_queue,
                                     completed_queue=amgr._completed_queue,
                                     rmgr=amgr._rmgr,
                                     rmq_conn_params=amgr._rmq_conn_params
                                     )

    write_session_description(amgr)

    desc = ru.read_json('%s/radical.entk.%s.json' % (amgr._sid, amgr._sid))
    # tasks are originally set but saved as a list in json
    # uses sorting for convenient comparison, this doesn't change validity
    for k, v in (desc['tree'].items()):
        if k.startswith("stage"):
            desc['tree'][k]['children'] = sorted(v['children'])

    src  = '%s/sample_data' % pwd

    assert desc == ru.read_json('%s/expected_desc_write_session.json' % src)


# ------------------------------------------------------------------------------
#
def test_get_session_description():

    sid  = 're.session.host.user.012345.1234/radical.entk.re.session.host.user.012345.1234'
    src  = '%s/sample_data/profiler' % pwd
    desc = get_session_description(sid=sid, src=src)

    assert desc == ru.read_json('%s/expected_desc_get_session.json' % src)


# ------------------------------------------------------------------------------
#
def generate_pipeline(nid):

    p       = Pipeline()
    s1      = Stage()
    s2      = Stage()
    t1      = Task()

    p.name  = 'p%s' % nid
    s1.name = 's1'
    s2.name = 's2'
    t1.name = 't1'

    t1.executable = '/bin/echo'
    t1.arguments  = ['hello']

    s1.add_tasks(t1)
    p.add_stages(s1)

    for cnt in range(10):

        tn            = Task()
        tn.name       = 't%s' % (cnt + 1)
        tn.executable = '/bin/echo'
        tn.arguments  = ['world']

        # Copy data from the task in first stage to the current task's location
        tn.copy_input_data = ['$Pipeline_%s_Stage_%s_Task_%s/output.txt'
                              % (p.name, s1.name, t1.name)]
        s2.add_tasks(tn)

    p.add_stages(s2)

    return p


# ------------------------------------------------------------------------------
#
def test_write_workflows():

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

        amgr          = AppManager(hostname=hostname, port=port,
                username=username, password=password)
        amgr.workflow = wf
        amgr._wfp     = WFprocessor(sid=amgr._sid,
                                    workflow=amgr._workflow,
                                    pending_queue=amgr._pending_queue,
                                    completed_queue=amgr._completed_queue,
                                    resubmit_failed=amgr._resubmit_failed,
                                    rmq_conn_params=amgr._rmq_conn_params)

        check = amgr.workflow

        # ----------------------------------------------------------------------
        # check json output, with defaut and custom fname
        for fname in [None, 'wf.json']:
            write_workflows(amgr.workflows, 'test', fname=fname)

            if not fname: fname = 'entk_workflow.json'
            data = ru.read_json('test/%s' % fname)

            check_stack(data['stack'])
            check_wf(data['workflows'][0], check)

            assert len(data['workflows']) == 1

            shutil.rmtree('test')


        # ----------------------------------------------------------------------
        # check with data return
        data = write_workflows(amgr.workflows, 'test', fwrite=False)

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
                                    resubmit_failed=amgr._resubmit_failed,
                                    rmq_conn_params=amgr._rmq_conn_params)
        check = amgr.workflows

        data = write_workflows(amgr.workflows, 'test', fwrite=False)

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
    test_write_workflows()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access

