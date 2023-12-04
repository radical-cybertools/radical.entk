# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

from unittest import TestCase

from radical.entk.execman.rp   import TaskManager      as RPTmgr
from radical.entk.execman.rp   import ResourceManager  as RPRmgr

import time
import threading as mt

try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
def _tmgr_side_effect(uid, rmgr, zmq_info):

    time.sleep(0.1)
    return True


class TestBase(TestCase):


    # ------------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.generate_id', return_value='tmgr.0000')
    @mock.patch('os.getcwd', return_value='test_folder')
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    @mock.patch('radical.utils.DebugHelper')
    def test_init(self, mocked_generate_id, mocked_getcwd, mocked_Logger,
                  mocked_Profiler, mocked_DebugHelper):

        rmgr = mock.MagicMock(spec=RPRmgr)

        RPTmgr._setup_zmq = lambda x, y: None
        tmgr = RPTmgr('test_tmgr', rmgr, {})
        self.assertIsNone(tmgr._rts_runner)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(RPTmgr, '__init__', return_value=None)
    @mock.patch('radical.pilot.TaskManager')
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    def test_start_manager(self, mocked_Profiler, mocked_Logger,
                           mocked_rp_tmgr, mocked_init):

        rmgr = mock.MagicMock(spec=RPRmgr)

        RPTmgr._setup_zmq = lambda x, y: None
        tmgr = RPTmgr('test_tmgr', rmgr, {})

        tmgr._log = mocked_Logger
        tmgr._prof = mocked_Profiler
        tmgr._uid = 'tmgr.0000'
        tmgr._rmgr = rmgr
        tmgr._tmgr = _tmgr_side_effect
        tmgr._zmq_info = {}

        tmgr._tmgr_terminate = None
        tmgr._tmgr_thread = None
        tmgr.start_manager()
        try:
            self.assertIsInstance(tmgr._tmgr_terminate, mt.Event)
            self.assertIsInstance(tmgr._tmgr_thread, mt.Thread)
            self.assertTrue(tmgr._tmgr_thread.is_alive())
        finally:
            if tmgr._tmgr_thread.is_alive():
                tmgr._tmgr_thread.join()

