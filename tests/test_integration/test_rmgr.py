
import os
import glob
import shutil
import pytest

import hypothesis.strategies as st
from   hypothesis import given, settings

import radical.utils as ru
import radical.pilot as rp

from radical.entk.execman.base import Base_ResourceManager as BaseRmgr
from radical.entk.execman.rp   import ResourceManager      as RPRmgr
from radical.entk.execman.mock import ResourceManager      as MockRmgr
from radical.entk              import exceptions           as ree

# Hypothesis settings
settings.register_profile("travis", max_examples=100, deadline=None)
settings.load_profile("travis")


# ------------------------------------------------------------------------------
#
@given(d=st.dictionaries(st.text(), st.text()))
def test_rmgr_base_initialization(d):

    try:
        home   = os.environ.get('HOME', '/home')
        folder = glob.glob('%s/.radical/utils/test.*' % home)

        for f in folder:
            shutil.rmtree(f)
    except:
        pass

    rmgr_id = ru.generate_id('test.%(item_counter)04d', ru.ID_CUSTOM)
    rmgr    = BaseRmgr(d, rmgr_id, None, {})

    assert rmgr._resource_desc == d
    assert rmgr._sid           == rmgr_id
    assert rmgr._rts           is None
    assert rmgr._rts_config    == {}
    assert rmgr.resource       is None
    assert rmgr.walltime       is None
    assert rmgr.cpus           == 1
    assert rmgr.gpus           == 0
    assert rmgr.project        is None
    assert rmgr.access_schema  is None
    assert rmgr.queue          is None
    assert rmgr._validated     is False

    assert rmgr._uid == 'resource_manager.0000'
    assert rmgr._logger
    assert rmgr._prof

    assert isinstance(rmgr.shared_data, list)


# ------------------------------------------------------------------------------
#
@given(s=st.characters(),
       l=st.lists(st.characters()),
       i=st.integers().filter(lambda x: type(x) == int),
       b=st.booleans(),
       se=st.sets(st.text()))
def test_rmgr_base_assignment_exception(s, l, i, b, se):

    data = [s, l, i, b, se]

    for d in data:
        with pytest.raises(ree.TypeError):
            BaseRmgr(d, 'test.0020', None, {})


# ------------------------------------------------------------------------------
#
def test_rmgr_base_get_resource_allocation_state():

    rmgr = BaseRmgr({}, 'test.0021', None, {})

    with pytest.raises(NotImplementedError):
        rmgr.get_resource_allocation_state()


# ------------------------------------------------------------------------------
#
def test_rmgr_base_completed_states():

    rmgr = BaseRmgr({}, 'test.0022', None, {})

    with pytest.raises(NotImplementedError):
        rmgr.get_completed_states()


# ------------------------------------------------------------------------------
#
@given(t=st.text(), i=st.integers())
def test_rmgr_base_validate_resource_desc(t, i):

    sid  = 'test.0014'
    rmgr = BaseRmgr({}, sid=sid, rts=None, rts_config={})

    with pytest.raises(ree.MissingError):
        rmgr._validate_resource_desc()

    res_dict = {'resource' : 'local.localhost',
                'walltime' : 30,
                'cpus'     : 20}

    with pytest.raises(ree.TypeError):

        res_dict = {'resource' : i,
                    'walltime' : t,
                    'cpus'     : t}

        rm = BaseRmgr(res_dict, sid=sid, rts=None, rts_config={})
        rm._validate_resource_desc()


    with pytest.raises(ree.TypeError):

        res_dict = {'resource' : t,
                    'walltime' : t,
                    'cpus'     : t}

        rm = BaseRmgr(res_dict, sid=sid, rts=None, rts_config={})
        rm._validate_resource_desc()


    with pytest.raises(ree.TypeError):

        res_dict = {'resource' : t,
                    'walltime' : i,
                    'cpus'     : t}

        rm = BaseRmgr(res_dict, sid=sid, rts=None, rts_config={})
        rm._validate_resource_desc()


    with pytest.raises(ree.TypeError):

        res_dict = {'resource' : t,
                    'walltime' : i,
                    'cpus'     : i,
                    'gpus'     : t}

        rm = BaseRmgr(res_dict, sid=sid, rts=None, rts_config={})
        rm._validate_resource_desc()


    with pytest.raises(ree.TypeError):

        res_dict = {'resource' : t,
                    'walltime' : i,
                    'cpus'     : i,
                    'gpus'     : i,
                    'project'  : i}

        rm = BaseRmgr(res_dict, sid=sid, rts=None, rts_config={})
        rm._validate_resource_desc()


    with pytest.raises(ree.TypeError):

        res_dict = {'resource'      : t,
                    'walltime'      : i,
                    'cpus'          : i,
                    'gpus'          : i,
                    'project'       : t,
                    'access_schema' : i}

        rm = BaseRmgr(res_dict, sid=sid, rts=None, rts_config={})
        rm._validate_resource_desc()


    with pytest.raises(ree.TypeError):

        res_dict = {'resource'      : t,
                    'walltime'      : i,
                    'cpus'          : i,
                    'gpus'          : i,
                    'project'       : t,
                    'access_schema' : t,
                    'queue'         : i}

        rm = BaseRmgr(res_dict, sid=sid, rts=None, rts_config={})
        rm._validate_resource_desc()


    if isinstance(t, str):

        res_dict = {'resource'      : t,
                    'walltime'      : i,
                    'cpus'          : i,
                    'gpus'          : i,
                    'project'       : t,
                    'access_schema' : t,
                    'queue'         : t}

        rm = BaseRmgr(res_dict, sid=sid, rts=None, rts_config={})
        assert rm._validate_resource_desc()


# ------------------------------------------------------------------------------
#
@given(t=st.text(), i=st.integers())
def test_rmgr_base_populate(t, i):

    if isinstance(t, str):

        res_dict = {'resource'      : t,
                    'walltime'      : i,
                    'cpus'          : i,
                    'gpus'          : i,
                    'project'       : t,
                    'access_schema' : t,
                    'queue'         : t}

        sid = 'test.0012'
        rm  = BaseRmgr(res_dict, sid=sid, rts=None, rts_config={})

        with pytest.raises(ree.EnTKError):
            rm._populate()

        rm._validate_resource_desc()
        rm._populate()


    res_dict = {'resource'      : 'local.localhost',
                'walltime'      : 40,
                'cpus'          : 100,
                'gpus'          : 25,
                'project'       : 'new',
                'queue'         : 'high',
                'access_schema' : 'gsissh'}


    rmgr_id = ru.generate_id('test.%(item_counter)04d', ru.ID_CUSTOM)
    rmgr    = BaseRmgr(res_dict, sid=rmgr_id, rts=None, rts_config={})

    rmgr._validate_resource_desc()
    rmgr._populate()

    assert rmgr._sid           == rmgr_id
    assert rmgr._resource      == 'local.localhost'
    assert rmgr._walltime      == 40
    assert rmgr._cpus          == 100
    assert rmgr._gpus          == 25
    assert rmgr._project       == 'new'
    assert rmgr._access_schema == 'gsissh'
    assert rmgr._queue         == 'high'
    assert rmgr._validated     is True


# ------------------------------------------------------------------------------
#
def test_rmgr_base_submit_resource_request():

    rmgr = BaseRmgr({}, 'test.0023', None, {})

    with pytest.raises(NotImplementedError):
        rmgr._submit_resource_request()


# ------------------------------------------------------------------------------
#
def test_rmgr_base_terminate_resource_request():

    rmgr = BaseRmgr({}, 'test.0024', None, {})

    with pytest.raises(NotImplementedError):
        rmgr._terminate_resource_request()


# ------------------------------------------------------------------------------
#
@given(d=st.dictionaries(st.text(), st.text()))
def test_rmgr_mock_initialization(d):

    try:
        home   = os.environ.get('HOME', '/home')
        folder = glob.glob('%s/.radical/utils/test.*' % home)

        for f in folder:
            shutil.rmtree(f)
    except:
        pass

    rmgr = MockRmgr(resource_desc=d, sid='test.0016')

    assert rmgr._resource_desc == d
    assert rmgr._sid           == 'test.0016'
    assert rmgr._rts           == 'mock'
    assert rmgr._resource      is None
    assert rmgr._walltime      is None
    assert rmgr._cpus          == 1
    assert rmgr._gpus          == 0
    assert rmgr._project       is None
    assert rmgr._access_schema is None
    assert rmgr._queue         is None
    assert rmgr._validated     is False
    assert rmgr._uid           == 'resource_manager.0000'

    assert rmgr._logger
    assert rmgr._prof

    assert isinstance(rmgr.shared_data, list)


# ------------------------------------------------------------------------------
#
def test_rmgr_mock_methods():

    rmgr = MockRmgr(resource_desc={}, sid='test.0025')

    assert rmgr._validate_resource_desc()

    assert not rmgr.get_resource_allocation_state()
    assert not rmgr.get_completed_states()
    assert not rmgr._populate()
    assert not rmgr._submit_resource_request()
    assert not rmgr._terminate_resource_request()


# ------------------------------------------------------------------------------
#
@given(d=st.dictionaries(st.text(), st.text()))
def test_rmgr_rp_initialization(d):

    with pytest.raises(ree.ValueError):
        sid  = ru.generate_id('test', ru.ID_UNIQUE)
        rmgr = RPRmgr(d, sid, rts_config={})

    config = {"sandbox_cleanup": False,
              "db_cleanup"     : False}

    try:
        home   = os.environ.get('HOME', '/home')
        folder = glob.glob('%s/.radical/utils/test.*' % home)

        for f in folder:
            shutil.rmtree(f)

    except:
        pass


    sid  = ru.generate_id('test', ru.ID_UNIQUE)
    rmgr = RPRmgr(d, sid, {'db_cleanup'     : False,
                           'sandbox_cleanup': False})

    assert rmgr._resource_desc       == d
    assert rmgr._sid                 == sid
    assert rmgr._rts                 == 'radical.pilot'
    assert rmgr._rts_config          == config
    assert rmgr._resource            is None
    assert rmgr._walltime            is None
    assert rmgr._cpus                == 1
    assert rmgr._gpus                == 0
    assert rmgr._project             is None
    assert rmgr._access_schema       is None
    assert rmgr._queue               is None
    assert rmgr._validated           is False
    assert rmgr._uid                 == 'resource_manager.0000'
    assert rmgr._download_rp_profile is False

    assert rmgr._logger
    assert rmgr._prof

    assert not rmgr.session
    assert not rmgr.pmgr
    assert not rmgr.pilot

    assert isinstance(rmgr.shared_data, list)


# ------------------------------------------------------------------------------
#
def test_rmgr_rp_completed_states():

    config = {"sandbox_cleanup": False,
              "db_cleanup"     : False}
    sid    = ru.generate_id('test', ru.ID_UNIQUE)
    rmgr   = RPRmgr({}, sid=sid, rts_config=config)

    assert rmgr.get_completed_states() == rp.FINAL


# ------------------------------------------------------------------------------

