from radical.entk.execman.base import Base_ResourceManager as BaseRmgr
from radical.entk.execman.rp import ResourceManager as RPRmgr
from radical.entk.execman.dummy import ResourceManager as DummyRmgr
import pytest
from hypothesis import given
import hypothesis.strategies as st
import os

@given(d=st.dictionaries(st.text(), st.text()))
def test_base_initialization(d):

    
    rmgr = BaseRmgr(d, 'test.0000', None)

    assert rmgr._resource_desc == d
    assert rmgr._resource == None
    assert rmgr._walltime == None
    assert rmgr._cores == None
    assert rmgr._project == None
    assert rmgr._access_schema == None
    assert rmgr._queue == None
    assert rmgr._rts == None
   
    # Shared data list
    assert isinstance(rmgr._shared_data, list)  

@given(d=st.dictionaries(st.text(), st.text()))
def test_rp_initialization(d):

    db_url = 'mongodb://138.201.86.166:27017/ee_exp_4c'
    os.environ['RADICAL_PILOT_DBURL'] = db_url

    rmgr = RPRmgr(d, 'test.0000')

    assert rmgr._resource_desc == d
    assert rmgr._resource == None
    assert rmgr._walltime == None
    assert rmgr._cores == None
    assert rmgr._project == None
    assert rmgr._access_schema == None
    assert rmgr._queue == None
    assert rmgr._rts == 'radical.pilot'
    assert rmgr._session == None
    assert rmgr._pmgr == None
    assert rmgr._pilot == None
    assert rmgr._download_rp_profile == False
    assert rmgr._sid == 'test.0000'
    assert rmgr._mlab_url == db_url
   
    # Shared data list
    assert isinstance(rmgr._shared_data, list)

@given(d=st.dictionaries(st.text(), st.text()))
def test_dummy_initialization(d):

    rmgr = DummyRmgr(resource_desc=d, sid='test.0000')

    assert rmgr._resource_desc == d
    assert rmgr._resource == None
    assert rmgr._walltime == None
    assert rmgr._cpus == 1
    assert rmgr._gpus == 0
    assert rmgr._project == None
    assert rmgr._access_schema == None
    assert rmgr._queue == None
    assert rmgr._rts == 'dummy'
   
    # Shared data list
    assert isinstance(rmgr._shared_data, list)


@given(s=st.characters(),
       l=st.lists(st.characters()),
       i=st.integers().filter(lambda x: type(x) == int),
       b=st.booleans(),
       se=st.sets(st.text()))
def test_assignment_exception(s,l,i,b,se):

    data = [s,l,i,b,se]

    for d in data:
        with pytest.raises(TypeError):
            rmgr = BaseRmgr(d,'test.0000')