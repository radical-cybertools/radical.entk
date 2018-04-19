from radical.entk import ResourceManager
from radical.entk.exceptions import *
import pytest, os
import radical.pilot as rp
from time import sleep


def test_validate_resource_description():

    """
    ***Purpose***: Test method that validates the resource description
    """

    res_dict = {
                    'resource': 'local.localhost',
                    'walltime': 40,
                    'cores': 20,
                    'project': 'Random'
                }
    
    os.environ['RADICAL_PILOT_DBURL'] = 'mlab-url'

    rm = ResourceManager(res_dict)

    new_res_dict = None
    with pytest.raises(TypeError):
        rm = ResourceManager(new_res_dict)
        rm._validate_resource_desc(sid='xyz')

    new_res_dict = {'a': 'b'}
    with pytest.raises(Error):
        rm = ResourceManager(new_res_dict)
        rm._validate_resource_desc(sid='xyz')

    data = [1, 'a']

    with pytest.raises(TypeError):

        res_dict = {
                    'resource': data[0],
                    'walltime': 40,
                    'cores': 20,
                    'project': 'Random'
                }
        rm = ResourceManager(res_dict)
        rm._validate_resource_desc(sid='xyz')

        res_dict = {
                    'resource': 'local.localhost',
                    'walltime': 40,
                    'cores': 20,
                    'project': data[0]
                }
        rm = ResourceManager(res_dict)
        rm._validate_resource_desc(sid='xyz')

        res_dict = {
                    'resource': 'local.localhost',
                    'walltime': data[1],
                    'cores': 20,
                    'project': 'Random'
                }
        rm = ResourceManager(res_dict)
        rm._validate_resource_desc(sid='xyz')

        res_dict = {
                    'resource': 'local.localhost',
                    'walltime': 40,
                    'cores': data[1],
                    'project': 'Random'
                }
        rm = ResourceManager(res_dict)
        rm._validate_resource_desc(sid='xyz')


def test_resource_manager_populate():

    """
    ***Purpose***: Test method that populates all the attributes of ResourceManager with a valid resource description.
    """

    res_dict = {
                    'resource': 'local.localhost',
                    'walltime': 40,
                    'cores': 20,
                    'project': 'Random'
                }

    os.environ['RADICAL_PILOT_DBURL'] = 'mlab-url'

    rm = ResourceManager(res_dict)

    with pytest.raises(Exception):

        rm._populate({  'resource': 'local.localhost',
                        'walltime': 40,
                        'cores': 20})


def test_resource_request():

    """
    ***Purpose***: Test the submission and cancelation of a resource request. Check states that pilot starts and 
    ends with.
    """
    
    res_dict = {
                    'resource': 'local.localhost',
                    'walltime': 40,
                    'cores': 20,
                    'project': ''
                }

    os.environ['RADICAL_PILOT_DBURL'] = 'mongodb://user:user@ds129013.mlab.com:29013/travis_tests'
    os.environ['RP_ENABLE_OLD_DEFINES'] = 'True'

    rm = ResourceManager(res_dict)
    rm._validate_resource_desc(sid='test')
    rm._populate()

    rm._submit_resource_request()

    assert rm.pilot.state == rp.PMGR_ACTIVE

    rm._cancel_resource_request()

    # State transition seems to be taking some time. So sleep
    sleep(30)

    assert rm.pilot.state == rp.CANCELED or rm.pilot.state == rp.DONE
    