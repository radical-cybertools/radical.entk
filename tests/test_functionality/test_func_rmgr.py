from radical.entk.execman.base.resource_manager import Base_ResourceManager as BaseRmgr
from radical.entk.execman.rp.resource_manager import ResourceManager as RPRmgr
from radical.entk.execman.dummy.resource_manager import ResourceManager as DummyRmgr
from radical.entk.exceptions import *
import os
import pytest
from hypothesis import given
import hypothesis.strategies as st
import radical.pilot as rp
from time import sleep


@given(t=st.text(), i=st.integers())
def test_validate_resource_description(t,i):
    """
    ***Purpose***: Test method that validates the resource description
    """
   
    os.environ['RADICAL_PILOT_DBURL'] = 'mlab-url'
    sid = 'test.0000'

    with pytest.raises(TypeError):

        res_dict = {
            'resource': i,
            'walltime': i,
            'cpus': i,
            'project': t
        }
        rm = BaseRmgr(res_dict, sid=sid, rts=None)
        rm._validate_resource_desc()

        res_dict = {
            'resource': t,
            'walltime': i,
            'cpus': i,
            'project': i
        }
        rm = BaseRmgr(res_dict,sid=sid, rts=None)
        rm._validate_resource_desc()

        res_dict = {
            'resource': t,
            'walltime': t,
            'cpus': i,
            'project': t
        }
        rm = BaseRmgr(res_dict,sid=sid, rts=None)
        rm._validate_resource_desc()

        res_dict = {
            'resource': t,
            'walltime': i,
            'cpus': t,
            'project': t
        }
        rm = BaseRmgr(res_dict,sid=sid, rts=None)
        rm._validate_resource_desc()



def test_resource_manager_initialization():

    """
    ***Purpose***:  Test initialization of all attributes of resource manager upon instantiation with a 
    resource description
    """

    res_dict = {
                    'resource': 'local.localhost',
                    'walltime': 40,
                    'cpus': 20,
                    'project': 'Random',
                    'gpus': 2,
                    'access_schema': 'gsissh',
                    'queue': 'normal'
                }

    
    os.environ['RADICAL_PILOT_DBURL'] = 'mlab-url'

    rm = BaseRmgr(res_dict, sid='test.0000', rts=None)

    with pytest.raises(EnTKError):
        rm._populate()

    rm._validate_resource_desc()
    rm._populate()

    
def test_rp_resource_request():
    """
    ***Purpose***: Test the submission and cancelation of a resource request. Check states that pilot starts and 
    ends with.
    """

    res_dict = {
                    'resource': 'local.localhost',
                    'walltime': 40,
                    'cpus': 1,
                    'project': ''
                }

    os.environ['RADICAL_PILOT_DBURL'] = 'mongodb://user:user@ds129013.mlab.com:29013/travis_tests'
    os.environ['RP_ENABLE_OLD_DEFINES'] = 'True'

    rm = RPRmgr(res_dict, sid='test.0000')
    rm._validate_resource_desc()
    rm._populate()

    rm._submit_resource_request()

    assert rm.pilot.state == rp.PMGR_ACTIVE

    rm._cancel_resource_request()

    # State transition seems to be taking some time. So sleep
    sleep(30)

    assert rm.pilot.state == rp.CANCELED or rm.pilot.state == rp.DONE


def test_dummy_resource_request():
    """
    ***Purpose***: Test the submission and cancelation of a resource request. Check states that pilot starts and 
    ends with.
    """

    res_dict = {
                    'resource': 'local.localhost',
                    'walltime': 40,
                    'cores': 20,
                }


    rm = DummyRmgr(resource_desc=res_dict, sid='test.0000')
    rm._validate_resource_desc()
    rm._populate()

    rm._submit_resource_request()
    rm._cancel_resource_request()