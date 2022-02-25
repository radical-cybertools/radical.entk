# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter, import-error

from unittest import TestCase, mock

import radical.entk.exceptions as ree

from radical.entk.execman.rp   import ResourceManager as RPRmgr


stage_ins = []


def _pilot_stage_in(sds):
    stage_ins.append(sds)


def _submit_pilot_side_effect(*args):
    mocked_Pilot = mock.MagicMock()
    mocked_Pilot.wait = mock.MagicMock(return_value='hello_from_pilot')
    mocked_Pilot.stage_in = _pilot_stage_in
    return mocked_Pilot


class TestBase(TestCase):


    # ------------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.generate_id', return_value='rmgr.0000')
    @mock.patch('os.getcwd', return_value='test_folder')
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    def test_init(self, mocked_generate_id, mocked_getcwd, mocked_Logger,
                  mocked_Profiler):

        rmgr = RPRmgr(resource_desc={'resource': 'localhost'},
                      sid='test.0000',
                      rts_config={"sandbox_cleanup": 'test_sandbox',
                                  "db_cleanup": False})
        self.assertIsNone(rmgr._session)
        self.assertIsNone(rmgr._pmgr)
        self.assertIsNone(rmgr._pilot)
        self.assertFalse(rmgr._download_rp_profile)

        with self.assertRaises(ree.ValueError):
            RPRmgr(resource_desc={'resource': 'localhost'},
                   sid='test.0000',
                   rts_config={"sandbox_cleanup": 'test_sandbox'})

        with self.assertRaises(ree.ValueError):
            RPRmgr(resource_desc={'resource': 'localhost'},
                   sid='test.0000', rts_config={"db_cleanup": False})

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(RPRmgr,'__init__', return_value=None)
    def test_session(self, mocked_init):

        rmgr = RPRmgr(resource_desc={'resource': 'localhost'},
                      sid='test.0000',
                      rts_config={"sandbox_cleanup": 'test_sandbox',
                                  "db_cleanup": False})
        rmgr._session = 'test_session'
        self.assertEqual(rmgr.session, 'test_session')


    @mock.patch.object(RPRmgr,'__init__', return_value=None)
    def test_pmgr(self, mocked_init):

        rmgr = RPRmgr(resource_desc={'resource': 'localhost'},
                      sid='test.0000',
                      rts_config={"sandbox_cleanup": 'test_sandbox',
                                  "db_cleanup": False})
        rmgr._pmgr = 'test_pmgr'
        self.assertEqual(rmgr.pmgr, 'test_pmgr')

    @mock.patch.object(RPRmgr,'__init__', return_value=None)
    def test_pilot(self, mocked_init):

        rmgr = RPRmgr(resource_desc={'resource': 'localhost'},
                      sid='test.0000',
                      rts_config={"sandbox_cleanup": 'test_sandbox',
                                  "db_cleanup": False})
        rmgr._pilot = 'test_pilot'
        self.assertEqual(rmgr.pilot, 'test_pilot')


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(RPRmgr,'__init__', return_value=None)
    def test_get_resource_allocation_state(self, mocked_init):

        rmgr = RPRmgr(resource_desc={'resource': 'localhost'},
                      sid='test.0000',
                      rts_config={"sandbox_cleanup": 'test_sandbox',
                                  "db_cleanup": False})
        rmgr._pilot = mock.Mock()
        rmgr._pilot.state = 'test_state'
        state = rmgr.get_resource_allocation_state()
        self.assertEqual(state, 'test_state')

        rmgr = RPRmgr(resource_desc={'resource': 'localhost'},
                      sid='test.0000',
                      rts_config={"sandbox_cleanup": 'test_sandbox',
                                  "db_cleanup": False})
        with self.assertRaises(AttributeError):
            state = rmgr.get_resource_allocation_state()


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(RPRmgr,'__init__', return_value=None)
    def test_get_completed_states(self, mocked_init):

        rmgr = RPRmgr(resource_desc={'resource': 'localhost'},
                      sid='test.0000',
                      rts_config={"sandbox_cleanup": 'test_sandbox',
                                  "db_cleanup": False})

        state = rmgr.get_completed_states()

        self.assertEqual(state, ['DONE', 'FAILED', 'CANCELED'])


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(RPRmgr,'__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    @mock.patch('radical.pilot.Session', return_value='test_session')
    @mock.patch('radical.pilot.PilotDescription', return_value='pilot_desc')
    @mock.patch('radical.pilot.PilotManager', return_value=mock.MagicMock(wait=mock.MagicMock(return_value=True),
                                                                          submit_pilots=_submit_pilot_side_effect,
                                                                          register_callback=mock.MagicMock(return_value=True)))
    def test_submit_resource_request(self, mocked_init, mocked_Logger,
                                     mocked_Profiler, mocked_Session,
                                     mocked_PilotDescription,
                                     mocked_PilotManager):

        rmgr = RPRmgr()
        rmgr._logger = mocked_Logger
        rmgr._prof = mocked_Profiler
        rmgr._uid = 'rmgr.0000'
        rmgr._sid = 'rmgr.0000'
        rmgr._rts_config = {'sandbox_cleanup': False}
        rmgr._resource = 'test_resource'
        rmgr._walltime = 30
        rmgr._cpus = 1
        rmgr._memory = 0
        rmgr._project = 'test_project'
        rmgr._gpus = 1
        rmgr._access_schema = 'test_access'
        rmgr._queue = 'test_queue'
        rmgr._shared_data = 'test_data'
        rmgr._outputs = 'test_outputs'
        rmgr._job_name = None
        rmgr._shared_data = ['test/file1.txt > file1.txt', 'file2.txt']

        rmgr.submit_resource_request()
        self.assertEqual(rmgr._session, 'test_session')
        self.assertEqual(stage_ins[0], [{'action': 'Transfer',
                                         'source': 'test/file1.txt',
                                         'target': 'file1.txt',
                                         'flags' : 2},
                                        {'action': 'Transfer',
                                         'source': 'file2.txt',
                                         'target': 'file2.txt',
                                         'flags' : 2}
                                        ])


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(RPRmgr,'__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    def test_get_rts_info(self, mocked_init, mocked_Logger,
                                     mocked_Profiler):
        rmgr = RPRmgr()
        rmgr._logger = mocked_Logger
        rmgr._prof = mocked_Profiler
        rmgr._pilot = mock.Mock()
        rmgr._pilot.as_dict = mock.MagicMock(return_value={'pilot': 'pilot.0000'})

        self.assertEqual(rmgr.get_rts_info(), {'pilot': 'pilot.0000'})
