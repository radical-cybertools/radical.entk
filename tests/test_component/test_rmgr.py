import radical.utils as ru
from radical.entk.execman.base import Base_ResourceManager as BaseRmgr
from radical.entk.execman.rp import ResourceManager as RPRmgr
from radical.entk.execman.mock import ResourceManager as MockRmgr
import pytest
from radical.entk.exceptions import *
from hypothesis import given, settings
import hypothesis.strategies as st
import os

# MLAB = 'mongodb://entk:entk123@ds143511.mlab.com:43511/entk_0_7_4_release'
MLAB = os.environ.get('RADICAL_PILOT_DBURL')

# Hypothesis settings
settings.register_profile("travis", max_examples=100, deadline=None)
settings.load_profile("travis")

@given(d=st.dictionaries(st.text(), st.text()))
def test_rmgr_base_initialization(d):

    try:
        import glob
        import shutil
        import os
        home = os.environ.get('HOME', '/home')
        test_fold = glob.glob('%s/.radical/utils/test.*' % home)
        for f in test_fold:
            shutil.rmtree(f)
    except Exception, ex:
        pass
    rmgr_id = ru.generate_id('test.%(item_counter)04d', ru.ID_CUSTOM)
    rmgr = BaseRmgr(d, rmgr_id, None, {})

    assert rmgr._resource_desc == d
    assert rmgr._sid == rmgr_id
    assert rmgr._rts == None
    assert rmgr._rts_config == {}
    assert rmgr.resource == None
    assert rmgr.walltime == None
    assert rmgr.cpus == 1
    assert rmgr.gpus == 0
    assert rmgr.project == None
    assert rmgr.access_schema == None
    assert rmgr.queue == None
    assert rmgr._validated == False

    assert rmgr._uid == 'resource_manager.0000'
    assert rmgr._logger
    assert rmgr._prof

    # Shared data list
    assert isinstance(rmgr.shared_data, list)


@given(s=st.characters(),
       l=st.lists(st.characters()),
       i=st.integers().filter(lambda x: type(x) == int),
       b=st.booleans(),
       se=st.sets(st.text()))
def test_rmgr_base_assignment_exception(s, l, i, b, se):

    data = [s, l, i, b, se]

    for d in data:
        with pytest.raises(TypeError):
            rmgr = BaseRmgr(d, 'test.0000', None, {})



def test_rmgr_base_get_resource_allocation_state():

    rmgr = BaseRmgr({}, 'test.0000', None, {})
    with pytest.raises(NotImplementedError):
        rmgr.get_resource_allocation_state()


def test_rmgr_base_completed_states():

    rmgr = BaseRmgr({}, 'test.0000', None, {})
    with pytest.raises(NotImplementedError):
        rmgr.get_completed_states()


@given(t=st.text(), i=st.integers())
def test_rmgr_base_validate_resource_desc(t, i):

    rmgr = BaseRmgr({}, sid='test.0000', rts=None, rts_config={})
    with pytest.raises(MissingError):
        rmgr._validate_resource_desc()

    sid = 'test.0000'

    res_dict = {
            'resource': 'local.localhost',
            'walltime': 30,
            'cpus': 20,
        }

    with pytest.raises(TypeError):

        res_dict = {
            'resource': i,
            'walltime': t,
            'cpus': t,
        }
        rm = BaseRmgr(res_dict, sid=sid, rts=None, rts_config={})
        rm._validate_resource_desc()

    with pytest.raises(TypeError):

        res_dict = {
            'resource': t,
            'walltime': t,
            'cpus': t,
        }
        rm = BaseRmgr(res_dict, sid=sid, rts=None, rts_config={})
        rm._validate_resource_desc()

    with pytest.raises(TypeError):

        res_dict = {
            'resource': t,
            'walltime': i,
            'cpus': t,
        }
        rm = BaseRmgr(res_dict, sid=sid, rts=None, rts_config={})
        rm._validate_resource_desc()

    with pytest.raises(TypeError):

        res_dict = {
            'resource': t,
            'walltime': i,
            'cpus': i,
            'gpus': t
        }
        rm = BaseRmgr(res_dict, sid=sid, rts=None, rts_config={})
        rm._validate_resource_desc()

    with pytest.raises(TypeError):

        res_dict = {
            'resource': t,
            'walltime': i,
            'cpus': i,
            'gpus': i,
            'project': i
        }
        rm = BaseRmgr(res_dict, sid=sid, rts=None, rts_config={})
        rm._validate_resource_desc()

    with pytest.raises(TypeError):

        res_dict = {
            'resource': t,
            'walltime': i,
            'cpus': i,
            'gpus': i,
            'project': t,
            'access_schema': i
        }
        rm = BaseRmgr(res_dict, sid=sid, rts=None, rts_config={})
        rm._validate_resource_desc()

    with pytest.raises(TypeError):

        res_dict = {
            'resource': t,
            'walltime': i,
            'cpus': i,
            'gpus': i,
            'project': t,
            'access_schema': t,
            'queue': i
        }
        rm = BaseRmgr(res_dict, sid=sid, rts=None, rts_config={})
        rm._validate_resource_desc()

    if isinstance(t, str):
        res_dict = {
            'resource': t,
            'walltime': i,
            'cpus': i,
            'gpus': i,
            'project': t,
            'access_schema': t,
            'queue': t
        }
        rm = BaseRmgr(res_dict, sid=sid, rts=None, rts_config={})
        assert rm._validate_resource_desc()


@given(t=st.text(), i=st.integers())
def test_rmgr_base_populate(t, i):

    if isinstance(t, str):
        res_dict = {
            'resource': t,
            'walltime': i,
            'cpus': i,
            'gpus': i,
            'project': t,
            'access_schema': t,
            'queue': t
        }
        rm = BaseRmgr(res_dict, sid=sid, rts=None, rts_config={})

        with pytest.raises(EnTKError):
            rm._populate()

        rm._validate_resource_desc()
        rm._populate()


    res_dict = {
        'resource': 'local.localhost',
                    'walltime': 40,
                    'cpus': 100,
                    'gpus': 25,
                    'project': 'new',
                    'queue': 'high',
                    'access_schema': 'gsissh'
    }

    rmgr_id = ru.generate_id('test.%(item_counter)04d', ru.ID_CUSTOM)
    rmgr = BaseRmgr(res_dict, sid=rmgr_id, rts=None, rts_config={})
    rmgr._validate_resource_desc()
    rmgr._populate()

    assert rmgr._sid == rmgr_id
    assert rmgr._resource == 'local.localhost'
    assert rmgr._walltime == 40
    assert rmgr._cpus == 100
    assert rmgr._gpus == 25
    assert rmgr._project == 'new'
    assert rmgr._access_schema == 'gsissh'
    assert rmgr._queue == 'high'
    assert rmgr._validated == True

def test_rmgr_base_submit_resource_request():

    rmgr = BaseRmgr({}, 'test.0000', None, {})
    with pytest.raises(NotImplementedError):
        rmgr._submit_resource_request()


def test_rmgr_base_terminate_resource_request():

    rmgr = BaseRmgr({}, 'test.0000', None, {})
    with pytest.raises(NotImplementedError):
        rmgr._terminate_resource_request()


@given(d=st.dictionaries(st.text(), st.text()))
def test_rmgr_mock_initialization(d):

    try:
        import glob
        import shutil
        import os
        home = os.environ.get('HOME', '/home')
        test_fold = glob.glob('%s/.radical/utils/test.*' % home)
        for f in test_fold:
            shutil.rmtree(f)
    except:
        pass

    rmgr = MockRmgr(resource_desc=d, sid='test.0000')

    assert rmgr._resource_desc == d
    assert rmgr._sid == 'test.0000'
    assert rmgr._rts == 'mock'
    assert rmgr._resource == None
    assert rmgr._walltime == None
    assert rmgr._cpus == 1
    assert rmgr._gpus == 0
    assert rmgr._project == None
    assert rmgr._access_schema == None
    assert rmgr._queue == None
    assert rmgr._validated == False

    assert rmgr._uid == 'resource_manager.0000'
    assert rmgr._logger
    assert rmgr._prof

    # Shared data list
    assert isinstance(rmgr.shared_data, list)


def test_rmgr_mock_methods():

    rmgr = MockRmgr(resource_desc={}, sid='test.0000')

    assert not rmgr.get_resource_allocation_state()
    assert not rmgr.get_completed_states()
    assert rmgr._validate_resource_desc()
    assert not rmgr._populate()
    assert not rmgr._submit_resource_request()
    assert not rmgr._terminate_resource_request()


@given(d=st.dictionaries(st.text(), st.text()))
def test_rmgr_rp_initialization(d):

    with pytest.raises(ValueError):
        rmgr = RPRmgr(d, 'test.0000', rts_config={})

    env_var = os.environ.get('RADICAL_PILOT_DBURL', None)
    if env_var:
        del os.environ['RADICAL_PILOT_DBURL']

    config={ "sandbox_cleanup": False,"db_cleanup": False}
    with pytest.raises(EnTKError):
        rmgr_id = ru.generate_id('test.%(item_counter)04d', ru.ID_CUSTOM)
        rmgr = RPRmgr(d, rmgr_id, {})


    try:
        import glob
        import shutil
        home = os.environ.get('HOME', '/home')
        test_fold = glob.glob('%s/.radical/utils/test.*' % home)
        for f in test_fold:
            shutil.rmtree(f)
    except:
        pass


    os.environ['RADICAL_PILOT_DBURL'] = MLAB
    rmgr_id = ru.generate_id('test.%(item_counter)04d', ru.ID_CUSTOM)
    rmgr = RPRmgr(d, rmgr_id, {'db_cleanup': False, 'sandbox_cleanup': False})

    assert rmgr._resource_desc == d
    assert rmgr._sid == rmgr_id
    assert rmgr._rts == 'radical.pilot'
    assert rmgr._rts_config == config
    assert rmgr._resource == None
    assert rmgr._walltime == None
    assert rmgr._cpus == 1
    assert rmgr._gpus == 0
    assert rmgr._project == None
    assert rmgr._access_schema == None
    assert rmgr._queue == None
    assert rmgr._validated == False

    assert rmgr._uid == 'resource_manager.0000'
    assert rmgr._logger
    assert rmgr._prof

    # Shared data list
    assert isinstance(rmgr.shared_data, list)

    assert not rmgr.session
    assert not rmgr.pmgr
    assert not rmgr.pilot
    assert rmgr._download_rp_profile == False
    assert rmgr._mlab_url




def test_rmgr_rp_completed_states():

    os.environ['RADICAL_PILOT_DBURL'] = MLAB
    config={ "sandbox_cleanup": False,"db_cleanup": False}
    rmgr = RPRmgr({}, sid='test.0000', rts_config=config)

    import radical.pilot as rp
    assert rmgr.get_completed_states() == [rp.CANCELED, rp.FAILED, rp.DONE]

