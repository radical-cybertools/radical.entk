#!/usr/bin/env python

import radical.utils         as ru
import radical.pilot         as rp

from radical.entk.execman.rp   import ResourceManager as RPRmgr


# ------------------------------------------------------------------------------
#
def test_rmgr_rp_get_resource_allocation_state():

    res_dict = {
        'resource': 'local.localhost',
                    'walltime': 10,
                    'cpus': 1,
                    'project': ''
    }

    config  = {'sandbox_cleanup': False, 'db_cleanup': False}
    rmgr_id = ru.generate_id('test.%(item_counter)04d', ru.ID_CUSTOM)
    rmgr    = RPRmgr(res_dict, sid=rmgr_id, rts_config=config)

    assert not rmgr.get_resource_allocation_state()

    rmgr._validate_resource_desc()
    rmgr._populate()
    rmgr._submit_resource_request()

    assert rmgr.get_resource_allocation_state() in [rp.PMGR_ACTIVE, rp.FAILED]
    rmgr._terminate_resource_request()


# ------------------------------------------------------------------------------
#
def test_rmgr_rp_resource_request():
    '''
    ***Purpose***: Test the submission and cancelation of a resource request.
                   Check states that pilot starts and ends with.
    '''

    res_dict = {'resource': 'local.localhost',
                'walltime': 10,
                'cpus'    : 1,
                'project' : ''
               }

  # os.environ['RP_ENABLE_OLD_DEFINES'] = 'True'

    config  = {'sandbox_cleanup': False, 'db_cleanup': False}
    rmgr_id = ru.generate_id('test.%(item_counter)04d', ru.ID_CUSTOM)
    rmgr    = RPRmgr(res_dict, sid=rmgr_id, rts_config=config)

    rmgr._validate_resource_desc()
    rmgr._populate()
    rmgr._submit_resource_request()
    rmgr._terminate_resource_request()

    # State transition seems to be taking some time. So sleep
    from time import sleep
    sleep(10)

    assert rmgr.get_resource_allocation_state() in rp.FINAL


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_rmgr_rp_get_resource_allocation_state()
    test_rmgr_rp_resource_request()


# ------------------------------------------------------------------------------

