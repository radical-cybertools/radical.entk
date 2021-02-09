# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

from unittest import TestCase

from radical.entk.execman.rp   import TaskManager as RPTmgr
from radical.entk.execman.rp   import ResourceManager as RPRmgr

import pika
import time
import psutil
import multiprocessing as mp
from pika.connection import ConnectionParameters

try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
def _tmgr_side_effect(uid, rmgr, pend_queue, comp_queue, rmq_params):

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
    @mock.patch('pika.BlockingConnection')
    def test_init(self, mocked_generate_id, mocked_getcwd, mocked_Logger,
                  mocked_Profiler, mocked_DebugHelper, mocked_BlockingConnection):

        mocked_BlockingConnection.channel = mock.MagicMock(spec=pika.BlockingConnection.channel)
        mocked_BlockingConnection.close = mock.MagicMock(return_value=None)
        mocked_BlockingConnection.channel.queue_delete = mock.MagicMock(return_value=None)
        mocked_BlockingConnection.channel.queue_declare = mock.MagicMock(return_value=None)
        rmq_params = mock.MagicMock(spec=ConnectionParameters)
        rmgr = mock.MagicMock(spec=RPRmgr)
        tmgr = RPTmgr('test_tmgr', ['pending_queues'], ['completed_queues'], 
                     rmgr, rmq_params)
        self.assertIsNone(tmgr._rts_runner)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(RPTmgr, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    def test_start_manager(self, mocked_init, mocked_Logger, mocked_Profiler):
        rmq_params = mock.MagicMock(spec=ConnectionParameters)
        rmgr = mock.MagicMock(spec=RPRmgr)
        tmgr = RPTmgr('test_tmgr', ['pending_queues'], ['completed_queues'], 
                     rmgr, rmq_params)

        tmgr._log = mocked_Logger
        tmgr._prof = mocked_Profiler
        tmgr._uid = 'tmgr.0000'
        tmgr._rmgr = 'test_rmgr'
        tmgr._rmq_conn_params = rmq_params
        tmgr._pending_queue = ['pending_queues']
        tmgr._completed_queue = ['completed_queues']
        tmgr._tmgr = _tmgr_side_effect

        tmgr._tmgr_terminate = None
        tmgr._tmgr_process = None
        tmgr.start_manager()
        try:
            self.assertIsInstance(tmgr._tmgr_terminate, mp.synchronize.Event)
            self.assertIsInstance(tmgr._tmgr_process, mp.context.Process)
            pid = tmgr._tmgr_process.pid
            self.assertTrue(psutil.pid_exists(pid))
        finally:
            if tmgr._tmgr_process.is_alive():
                tmgr._tmgr_process.join()
