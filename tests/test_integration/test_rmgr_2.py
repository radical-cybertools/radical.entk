
import os
import time

import radical.utils as ru
import radical.pilot as rp

from radical.entk.execman.rp import ResourceManager as RPRmgr


# ------------------------------------------------------------------------------
#
def test_rmgr_rp_get_resource_allocation_state():

    res_dict = {'resource'        : 'local.localhost',
                'walltime'        : 10,
                'cpus'            : 1,
                'project'         : ''}
    config   = {"sandbox_cleanup" : False,
                "db_cleanup"      : False}
    sid      = ru.generate_id('test', ru.ID_UNIQUE)
    rmgr     = RPRmgr(res_dict, sid=sid, rts_config=config)

    assert not rmgr.get_resource_allocation_state()

    rmgr._validate_resource_desc()
    rmgr._populate()
    rmgr._submit_resource_request()

    assert rmgr.get_resource_allocation_state() in [rp.PMGR_ACTIVE, rp.FAILED]

    rmgr._terminate_resource_request()


# ------------------------------------------------------------------------------
#
def test_rmgr_rp_resource_request():
    """
    ***Purpose***: Test the submission and cancelation of a resource request.
                   Check states that pilot starts and ends with.
    """

    res_dict = {'resource'        : 'local.localhost',
                'walltime'        : 10,
                'cpus'            : 1,
                'project'         : ''}
    config   = {"sandbox_cleanup" : False,
                "db_cleanup"      : False}
    sid      = ru.generate_id('test', ru.ID_UNIQUE)
    rmgr     = RPRmgr(res_dict, sid=sid, rts_config = config)

    rmgr._validate_resource_desc()
    rmgr._populate()
    rmgr._submit_resource_request()
    rmgr._terminate_resource_request()

    t_0 = time.time()
    while time.time() - t_0 < 30:

        state = rmgr.get_resource_allocation_state()
        if state in rp.FINAL:
            break
        time.sleep(1)

    assert state in [rp.CANCELED, rp.FAILED, rp.DONE]


# ------------------------------------------------------------------------------

