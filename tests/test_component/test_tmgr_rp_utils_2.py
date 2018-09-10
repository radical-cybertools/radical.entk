from radical.entk.execman.rp.task_processor import *
from radical.entk.exceptions import *
from radical.entk import Task, Stage, Pipeline
import radical.pilot as rp
import os
import pytest
import shutil
import glob
import radical.utils as ru

# MLAB = 'mongodb://entk:entk123@ds143511.mlab.com:43511/entk_0_7_4_release'
MLAB = os.environ.get('RADICAL_PILOT_DBURL')

def test_create_task_from_cu():
    """
    **Purpose**: Test if the 'create_task_from_cu' function generates a Task with the correct uid, parent_stage and
    parent_pipeline from a RP ComputeUnit
    """

    session = rp.Session(dburl=MLAB)
    umgr = rp.UnitManager(session=session)
    cud = rp.ComputeUnitDescription()
    cud.name = 'uid, name, parent_stage_uid, parent_stage_name, parent_pipeline_uid, parent_pipeline_name'
    cud.executable = '/bin/echo'

    cu = rp.ComputeUnit(umgr, cud)

    t = create_task_from_cu(cu)

    assert t.uid == 'uid'
    assert t.name == 'name'
    assert t.parent_stage['uid'] == 'parent_stage_uid'
    assert t.parent_stage['name'] == 'parent_stage_name'
    assert t.parent_pipeline['uid'] == 'parent_pipeline_uid'
    assert t.parent_pipeline['name'] == 'parent_pipeline_name'

