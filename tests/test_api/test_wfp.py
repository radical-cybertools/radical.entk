from radical.entk.appman.wfprocessor import WFprocessor
from radical.entk import Pipeline, Stage, Task
import pytest
from radical.entk.exceptions import *
import os
from hypothesis import given, strategies as st


@given(s=st.characters(),
       i=st.integers().filter(lambda x: type(x) == int),
       b=st.booleans(),
       l=st.lists(st.characters()))
def test_initialization(s, i, b, l):

    p = Pipeline()
    st = Stage()
    t = Task()
    t.executable = ['/bin/date']
    st.add_tasks(t)
    p.add_stages(st)

    wfp = WFprocessor(sid='rp.session.local.0000',
                      workflow=set([p]),
                      pending_queue=['pending'],
                      completed_queue=['completed'],
                      mq_hostname='hostname',
                      port=567,
                      resubmit_failed=True)

    assert 'radical.entk.wfprocessor' in wfp._uid
    assert wfp._pending_queue == ['pending']
    assert wfp._completed_queue == ['completed']
    assert wfp._mq_hostname == 'hostname'
    assert wfp._port == 567
    assert wfp._wfp_process == None
    assert wfp._workflow == set([p])

    if not isinstance(s, unicode):
        wfp = WFprocessor(sid=s,
                          workflow=set([p]),
                          pending_queue=l,
                          completed_queue=l,
                          mq_hostname=s,
                          port=i,
                          resubmit_failed=b)


@given(s=st.characters(),
       l=st.lists(st.characters()),
       i=st.integers().filter(lambda x: type(x) == int),
       b=st.booleans(),
       se=st.sets(st.text()),
       di=st.dictionaries(st.text(), st.text()))
def test_assignment_exceptions(s, l, i, b, se, di):

    p = Pipeline()
    st = Stage()
    t = Task()
    t.executable = ['/bin/date']
    st.add_tasks(t)
    p.add_stages(st)

    data_type = [s, l, i, b, se, di]

    for d in data_type:

        if not isinstance(d, str):

            with pytest.raises(TypeError):

                wfp = WFprocessor(sid=d,
                                  workflow=set([p]),
                                  pending_queue=d,
                                  completed_queue=d,
                                  mq_hostname=d,
                                  port=d,
                                  resubmit_failed=d)
