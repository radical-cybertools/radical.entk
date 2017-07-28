from radical.entk import ResourceManager
from radical.entk.exceptions import *
import pytest, os
import radical.pilot as rp
from time import sleep


def test_resource_manager_initialization():

    """
    ***Purpose***:  Test initialization of all attributes of resource manager upon instantiation with a 
    resource description
    """

    res_dict = {
                    'resource': 'local.localhost',
                    'walltime': 40,
                    'cores': 20,
                    'project': 'Random'
                }

    if not os.environ.get('RADICAL_PILOT_DBURL', None):
        with pytest.raises(Error):
            rm = ResourceManager(res_dict)
    
    os.environ['RADICAL_PILOT_DBURL'] = 'mlab-url'

    rm = ResourceManager(res_dict)

    assert rm.resource == res_dict['resource']
    assert rm.walltime == res_dict['walltime']
    assert rm.cores == res_dict['cores']
    assert rm.project == res_dict['project']


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
        rm._validate_resource_desc(new_res_dict)

    new_res_dict = {'a': 'b'}

    with pytest.raises(Error):
        rm._validate_resource_desc(new_res_dict)

    data = [1, 'a']

    with pytest.raises(TypeError):

        res_dict = {
                    'resource': data[0],
                    'walltime': 40,
                    'cores': 20,
                    'project': 'Random'
                }

        rm._validate_resource_desc(res_dict)

        res_dict = {
                    'resource': 'local.localhost',
                    'walltime': 40,
                    'cores': 20,
                    'project': data[0]
                }

        rm._validate_resource_desc(res_dict)

        res_dict = {
                    'resource': 'local.localhost',
                    'walltime': data[1],
                    'cores': 20,
                    'project': 'Random'
                }

        rm._validate_resource_desc(res_dict)

        res_dict = {
                    'resource': 'local.localhost',
                    'walltime': 40,
                    'cores': data[1],
                    'project': 'Random'
                }

        rm._validate_resource_desc(res_dict)


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

    os.environ['RADICAL_PILOT_DBURL'] = 'mongodb://entk:entk@ds129010.mlab.com:29010/test_entk'
    os.environ['RP_ENABLE_OLD_DEFINES'] = 'True'

    rm = ResourceManager(res_dict)

    rm._submit_resource_request()

    assert rm.pilot.state == rp.ACTIVE

    rm._cancel_resource_request()

    # State transition seems to be taking some time. So sleep
    sleep(30)

    assert rm.pilot.state == rp.CANCELED or rm.pilot.state == rp.DONE
    