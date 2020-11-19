# pylint: disable=protected-access, unused-argument, eval-used
# pylint: disable=no-value-for-parameter
import os

from unittest import TestCase

import radical.utils
import radical.entk.states as states

from radical.entk.utils              import get_session_description
from radical.entk.utils              import write_session_description
from radical.entk.utils              import write_workflows

try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
class TestBase(TestCase):


    # ------------------------------------------------------------------------------
    #
    def test_write_session_description(self):
        global_jsons = []

        def _write_json_side_effect(desc, path):
            nonlocal global_jsons
            global_jsons.append([desc, path])

        curr_path = os.path.dirname(__file__)
        with open(curr_path + '/sample_data/expected_desc_write_session.dict') as session:
            data = session.readlines()

        expected_session = eval(''.join([x for x in data]))
        radical.utils.write_json = mock.MagicMock(side_effect=_write_json_side_effect)
        amgr = mock.Mock()
        amgr.resource_desc = {'resource' : 'xsede.stampede',
                              'walltime' : 59,
                              'cpus'     : 128,
                              'gpus'     : 64,
                              'project'  : 'xyz',
                              'queue'    : 'high'}
        amgr._uid = 'amgr.0000'
        amgr.sid = 'test_amgr'
        amgr._wfp = mock.Mock()
        amgr._wfp._uid = 'wfp.0000'
        amgr._rmgr = mock.Mock()
        amgr._rmgr._uid = 'rmgr.0000'
        amgr._task_manager = mock.Mock()
        amgr._task_manager._uid = 'tmgr.0000'

        pipe = mock.Mock()
        pipe.uid = 'pipe.0000'
        pipe.name = 'pipe.0000'
        pipe.state_history = ['DESCRIBED']

        stage = mock.Mock()
        stage.uid = 'stage.0000'
        stage.name = 'stage.0000'
        stage.state_history = ['DESCRIBED']

        task = mock.Mock()
        task.uid = 'task.0000'
        task.name = 'task.0000'
        task.rts_uid = 'unit.000000'
        task.state_history = ['DESCRIBED']

        stage.tasks = [task]
        pipe.stages = [stage]
        amgr._workflows = [[pipe]]

        write_session_description(amgr)
        self.assertEqual(global_jsons[0][0], expected_session)
        self.assertEqual(global_jsons[0][1], 'test_amgr/radical.entk.test_amgr.json')


    # ------------------------------------------------------------------------------
    #
    def test_get_session_description(self):
        pwd = os.path.dirname(__file__)
        sid  = 're.session.host.user.012345.1234/radical.entk.re.session.host.user.012345.1234'
        src  = '%s/sample_data/profiler' % pwd
        desc = get_session_description(sid=sid, src=src)

        self.assertEqual(desc, radical.utils.read_json('%s/expected_desc_get_session.json' % src))


    # ------------------------------------------------------------------------------
    #
    def test_write_workflows(self):

        stack = {'sys': {'python': '3.8.5', 'pythonpath': '', 'virtualenv': 'rct_test'},
                 'radical': {'radical.utils': '1.5.4',
                 'radical.saga': '1.5.6',
                 'radical.pilot': '1.5.5',
                 'radical.entk': '1.5.5',
                 'radical.gtod': '1.5.0'}}
        radical.utils.stack = mock.MagicMock(return_value=stack)

        pipe = mock.Mock()
        pipe.uid = 'pipe.0000'
        pipe.name = 'pipe.0000'
        pipe.state = states.INITIAL
        pipe.state_history = [states.INITIAL]
        pipe.completed = False
        pipe.current_stage = 1

        stage = mock.Mock()
        stage.uid = 'stage.0000'
        stage.name = 'stage.0000'
        stage.state = states.SCHEDULING
        stage.state_history = [states.SCHEDULING]

        task = mock.Mock()
        task.uid = 'task.0000'
        task.name = 'task.0000'
        task.state = states.INITIAL
        task.state_history = [states.INITIAL]
        task.to_dict = mock.MagicMock(return_value={'uid'           : 'task.0000',
                                                    'name'          : 'task.0000',
                                                    'state'         : states.INITIAL,
                                                    'state_history' : [states.INITIAL],
                                                })


        stage.tasks = [task]
        pipe.stages = [stage]
        workflow = [[pipe]]

        workflow = write_workflows(workflow, 'test', fwrite=False)
        curr_path = os.path.dirname(__file__)
        with open(curr_path + '/sample_data/expected_workflow.dict') as session:
            data = session.readlines()

        expected_workflow = eval(''.join([x for x in data]))

        self.assertEqual(workflow, expected_workflow)
# ------------------------------------------------------------------------------
# pylint: enable=protected-access
