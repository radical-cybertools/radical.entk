from radical.entk import ResourceManager as Rmgr
from radical.entk.exceptions import *
import pytest
from hypothesis import given
import hypothesis.strategies as st
import radical.utils as ru
import os

@given(d=st.dictionaries(st.text(), st.text()))
def test_initialization(d):

    rmgr = Rmgr(d)

    assert rmgr._resource_desc == d
    assert rmgr._session == None
    assert rmgr._pmgr == None
    assert rmgr._pilot == None
    assert rmgr._resource == None
    assert rmgr._walltime == None
    assert rmgr._cores == None
    assert rmgr._project == None
    assert rmgr._access_schema == None
    assert rmgr._queue == None


    assert rmgr._mlab_url == os.environ.get('RADICAL_PILOT_DBURL', None)
    
    # Shared data list
    assert isinstance(rmgr._shared_data, list)


@given(t=st.text(),
       l=st.lists(st.text()),
       i=st.integers().filter(lambda x: type(x) == int),
       b=st.booleans(),
       se=st.sets(st.text()),
       d=st.dictionaries(st.text(),st.text()))
def test_assignment_exceptions(t,l,i,b,se,d):
    
    rmgr = Rmgr(d)
    