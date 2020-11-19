# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

from unittest import TestCase

from radical.entk.execman.base   import Base_TaskManager as Tmgr
from radical.entk.execman.base   import Base_ResourceManager

from pika.connection import ConnectionParameters

import pika
import threading as mt

try:
    import mock
except ImportError:
    from unittest import mock


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
        rmgr = mock.MagicMock(spec=Base_ResourceManager)
        tmgr = Tmgr('test_tmgr', ['pending_queues'], ['completed_queues'], 
                     rmgr, rmq_params, 'test_rts')

        self.assertEqual(tmgr._sid, 'test_tmgr')
        self.assertEqual(tmgr._pending_queue, ['pending_queues'])
        self.assertEqual(tmgr._completed_queue, ['completed_queues'])
        self.assertEqual(tmgr._rmgr, rmgr)
        self.assertEqual(tmgr._rts, 'test_rts')
        self.assertEqual(tmgr._rmq_conn_params, rmq_params)
        self.assertEqual(tmgr._uid, 'tmgr.0000')
        self.assertEqual(tmgr._path, 'test_folder/test_tmgr')
        self.assertEqual(tmgr._hb_request_q, 'test_tmgr-hb-request')
        self.assertEqual(tmgr._hb_response_q, 'test_tmgr-hb-response')
        self.assertIsNone(tmgr._tmgr_process)
        self.assertIsNone(tmgr._tmgr_terminate)
        self.assertIsNone(tmgr._hb_thread)
        self.assertIsNone(tmgr._hb_terminate)
        self.assertEqual(tmgr._hb_interval, 30)

        with self.assertRaises(TypeError):
            tmgr = Tmgr(25, ['pending_queues'], ['completed_queues'], 
                        rmgr, rmq_params, 'test_rts')

        with self.assertRaises(TypeError):
            tmgr = Tmgr('test_tmgr', 'pending_queues', ['completed_queues'], 
                     rmgr, rmq_params, 'test_rts')

        with self.assertRaises(TypeError):
            tmgr = Tmgr('test_tmgr', ['pending_queues'], 'completed_queues', 
                     rmgr, rmq_params, 'test_rts')

        with self.assertRaises(TypeError):
            tmgr = Tmgr('test_tmgr', ['pending_queues'], ['completed_queues'], 
                     'test_rmq', rmq_params, 'test_rts')

        with self.assertRaises(TypeError):
            tmgr = Tmgr('test_tmgr', ['pending_queues'], ['completed_queues'], 
                     rmgr, 'test_params', 'test_rts')

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Tmgr, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    @mock.patch('pika.BlockingConnection')
    def test_advance(self, mocked_init, mocked_Logger, mocked_Profiler,
                     mocked_BlockingConnection):

        mocked_BlockingConnection.channel = mock.MagicMock(spec=pika.BlockingConnection.channel)
        mocked_BlockingConnection.close = mock.MagicMock(return_value=None)
        mocked_BlockingConnection.channel.queue_delete = mock.MagicMock(return_value=None)
        mocked_BlockingConnection.channel.queue_declare = mock.MagicMock(return_value=None)
        rmq_params = mock.MagicMock(spec=ConnectionParameters)
        rmgr = mock.MagicMock(spec=Base_ResourceManager)
        tmgr = Tmgr('test_tmgr', ['pending_queues'], ['completed_queues'], 
                     rmgr, rmq_params, 'test_rts')

        global_syncs = []

        def _sync_side_effect(log_entry, uid, state, msg):
            nonlocal global_syncs
            global_syncs.append([log_entry, uid, state, msg])

        tmgr._log = mocked_Logger
        tmgr._prof = mocked_Profiler
        tmgr._sync_with_master = mock.MagicMock(side_effect=_sync_side_effect)
        tmgr._uid = 'tmgr.0000'
        obj = mock.Mock()
        obj.parent_stage = {'uid': 'test_stage'}
        obj.parent_pipeline = {'uid': 'test_pipe'}
        obj.uid = 'test_object'
        obj.state = 'test_state'
        tmgr._advance(obj, 'Task', None, 'channel','queue')
        self.assertEqual(global_syncs[0],[obj, 'Task', 'channel','queue'])
        self.assertIsNone(obj.state)
        global_syncs = []
        tmgr._advance(obj, 'Stage', 'new_state', 'channel','queue')
        self.assertEqual(global_syncs[0],[obj, 'Stage', 'channel','queue'])
        self.assertEqual(obj.state, 'new_state')

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Tmgr, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    def test_start_heartbeat(self, mocked_init, mocked_Logger, mocked_Profiler):

        rmq_params = mock.MagicMock(spec=ConnectionParameters)
        rmgr = mock.MagicMock(spec=Base_ResourceManager)
        tmgr = Tmgr('test_tmgr', ['pending_queues'], ['completed_queues'], 
                     rmgr, rmq_params, 'test_rts')

        global_boolean = False

        def _heartbeat_side_effect():
            nonlocal global_boolean
            global_boolean = True

        tmgr._log = mocked_Logger
        tmgr._prof = mocked_Profiler
        tmgr._heartbeat = mock.MagicMock(side_effect=_heartbeat_side_effect)
        tmgr._uid = 'tmgr.0000'
        tmgr._hb_terminate = None
        tmgr._hb_thread = None

        tmgr.start_heartbeat()

        try:
            self.assertTrue(global_boolean)
            self.assertIsInstance(tmgr._hb_terminate, mt.Event)
            self.assertIsInstance(tmgr._hb_thread, mt.Thread)
        finally:
            if tmgr._hb_thread.is_alive():
                tmgr._hb_thread.join()
