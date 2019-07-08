
import pytest

import radical.pilot as rp

from radical.entk.execman.rp.task_processor import create_task_from_cu
from radical.entk.execman.rp.task_processor import resolve_arguments
from radical.entk.execman.rp.task_processor import resolve_tags
from radical.entk.exceptions                import EnTKError


# ------------------------------------------------------------------------------
#
def test_create_task_from_cu():
    """
    **Purpose**: Test if the 'create_task_from_cu' function generates a Task with the correct uid, parent_stage and
    parent_pipeline from a RP ComputeUnit
    """

    session        = rp.Session()
    umgr           = rp.UnitManager(session=session)
    cud            = rp.ComputeUnitDescription()
    cud.name       = 'uid, name, parent_stage_uid, parent_stage_name, ' \
                     'parent_pipeline_uid, parent_pipeline_name'
    cud.executable = '/bin/echo'

    cu = rp.ComputeUnit(umgr, cud)
    t  = create_task_from_cu(cu)

    assert t.uid                     == 'uid'
    assert t.name                    == 'name'
    assert t.parent_stage['uid']     == 'parent_stage_uid'
    assert t.parent_stage['name']    == 'parent_stage_name'
    assert t.parent_pipeline['uid']  == 'parent_pipeline_uid'
    assert t.parent_pipeline['name'] == 'parent_pipeline_name'

    session.close()


# ------------------------------------------------------------------------------
#
def test_resolve_args():

    pipeline_name = 'p1'
    stage_name    = 's1'
    t1_name       = 't1'
    t2_name       = 't2'

    placeholders = {
        pipeline_name: {
            stage_name: {
                t1_name: {
                    'path'   : '/home/vivek/t1',
                    'rts_uid': 'unit.0002'
                },
                t2_name: {
                    'path'   : '/home/vivek/t2',
                    'rts_uid': 'unit.0003'
                }
            }
        }
    }

    arguments = [
        '$SHARED',
        '$Pipeline_%s_Stage_%s_Task_%s' % (pipeline_name, stage_name, t1_name),
        '$Pipeline_%s_Stage_%s_Task_%s' % (pipeline_name, stage_name, t2_name),
        '$NODE_LFS_PATH/test.txt'
    ]

    resolved = resolve_arguments(arguments, placeholders)
    assert resolved == ['$RP_PILOT_STAGING',
                        '/home/vivek/t1',
                        '/home/vivek/t2',
                        '$NODE_LFS_PATH/test.txt']


# ------------------------------------------------------------------------------
#
def test_resolve_tags():

    pipeline_name = 'p1'
    stage_name    = 's1'
    t1_name       = 't1'
    t2_name       = 't2'

    placeholders = {
        pipeline_name: {
            stage_name: {
                t1_name: {
                    'path'   : '/home/vivek/t1',
                    'rts_uid': 'unit.0002'
                },
                t2_name: {
                    'path'   : '/home/vivek/t2',
                    'rts_uid': 'unit.0003'
                }
            }
        }
    }

    resolved = resolve_tags(tag=t1_name, parent_pipeline_name=pipeline_name,
                            placeholders=placeholders)
    assert resolved == 'unit.0002'

    with pytest.raises(EnTKError):
        resolve_tags(tag='t3', parent_pipeline_name=pipeline_name,
                     placeholders=placeholders)


# ------------------------------------------------------------------------------

