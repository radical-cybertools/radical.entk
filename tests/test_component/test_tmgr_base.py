# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

from unittest import TestCase

from radical.entk.execman.base   import Base_TaskManager as Tmgr
from radical.entk.execman.base   import Base_ResourceManager

from pika.connection import ConnectionParameters

import pika
import time
import psutil
import threading as mt
import multiprocessing as mp

try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
def _tmgr_side_effect(event):

    while not event.is_set():
        time.sleep(0.1)
    return True


class TestBase(TestCase):

    # --------------------------------------------------------------------------
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

        def _sync_side_effect(obj, obj_type, channel, conn_params, queue):
            nonlocal global_syncs
            global_syncs.append([obj, obj_type, channel, conn_params, queue])

        tmgr._log = mocked_Logger
        tmgr._prof = mocked_Profiler
        tmgr._sync_with_master = mock.MagicMock(side_effect=_sync_side_effect)
        tmgr._uid = 'tmgr.0000'
        obj = mock.Mock()
        obj.parent_stage = {'uid': 'test_stage'}
        obj.parent_pipeline = {'uid': 'test_pipe'}
        obj.uid = 'test_object'
        obj.state = 'test_state'
        tmgr._advance(obj, 'Task', None, 'channel','params','queue')
        self.assertEqual(global_syncs[0],[obj, 'Task', 'channel', 'params', 'queue'])
        self.assertIsNone(obj.state)
        global_syncs = []
        tmgr._advance(obj, 'Stage', 'new_state', 'channel', 'params', 'queue')
        self.assertEqual(global_syncs[0],[obj, 'Stage', 'channel', 'params', 
                                          'queue'])
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

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Tmgr, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    def test_check_manager(self, mocked_init, mocked_Logger, mocked_Profiler):

        rmq_params = mock.MagicMock(spec=ConnectionParameters)
        rmgr = mock.MagicMock(spec=Base_ResourceManager)
        tmgr = Tmgr('test_tmgr', ['pending_queues'], ['completed_queues'], 
                     rmgr, rmq_params, 'test_rts')

        def _tmgr_side_effect(amount):
            time.sleep(amount)

        tmgr._tmgr_process = mt.Thread(target=_tmgr_side_effect,
                                       name='test_tmgr', args=(1,))
        tmgr._tmgr_process.start()

        self.assertTrue(tmgr.check_manager())
        tmgr._tmgr_process.join()
        self.assertFalse(tmgr.check_manager())

        tmgr._tmgr_process = None
        self.assertFalse(tmgr.check_manager())

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Tmgr, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    def test_terminate_manager(self, mocked_init, mocked_Logger, mocked_Profiler):

        rmq_params = mock.MagicMock(spec=ConnectionParameters)
        rmgr = mock.MagicMock(spec=Base_ResourceManager)
        tmgr = Tmgr('test_tmgr', ['pending_queues'], ['completed_queues'], 
                     rmgr, rmq_params, 'test_rts')

        tmgr._log = mocked_Logger
        tmgr._prof = mocked_Profiler
        tmgr._uid = 'tmgr.0000'

        tmgr._tmgr_terminate = mp.Event()

        tmgr._tmgr_process = mp.Process(target=_tmgr_side_effect,
                                       name='test_tmgr',
                                       args=(tmgr._tmgr_terminate,))
        tmgr._tmgr_process.start()

        pid = tmgr._tmgr_process.pid
        tmgr.check_manager = mock.MagicMock(return_value=True)
        tmgr.terminate_manager()

        self.assertIsNone(tmgr._tmgr_process)

        self.assertFalse(psutil.pid_exists(pid))

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Tmgr, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    def test_check_heartbeat(self, mocked_init, mocked_Logger, mocked_Profiler):

        rmq_params = mock.MagicMock(spec=ConnectionParameters)
        rmgr = mock.MagicMock(spec=Base_ResourceManager)
        tmgr = Tmgr('test_tmgr', ['pending_queues'], ['completed_queues'], 
                     rmgr, rmq_params, 'test_rts')

        def _tmgr_side_effect(amount):
            time.sleep(amount)

        tmgr._hb_thread = mt.Thread(target=_tmgr_side_effect,
                                       name='test_tmgr', args=(1))
        tmgr._hb_thread.start()

        self.assertTrue(tmgr.check_heartbeat())
        tmgr._hb_thread.join()
        self.assertFalse(tmgr.check_heartbeat())

        tmgr._hb_thread = None
        self.assertFalse(tmgr.check_heartbeat())

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Tmgr, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    @mock.patch('pika.BlockingConnection')
    def test_terminate_heartbeat(self, mocked_init, mocked_Logger, mocked_Profiler,
                     mocked_BlockingConnection):

        mocked_BlockingConnection.channel = mock.MagicMock(spec=pika.BlockingConnection.channel)
        mocked_BlockingConnection.close = mock.MagicMock(return_value=None)
        mocked_BlockingConnection.channel.queue_delete = mock.MagicMock(return_value=None)
        mocked_BlockingConnection.channel.queue_declare = mock.MagicMock(return_value=None)
        mocked_BlockingConnection.close = mock.MagicMock(return_value=None)
        rmq_params = mock.MagicMock(spec=ConnectionParameters)
        rmgr = mock.MagicMock(spec=Base_ResourceManager)
        tmgr = Tmgr('test_tmgr', ['pending_queues'], ['completed_queues'], 
                     rmgr, rmq_params, 'test_rts')

        tmgr._rmq_conn_params = rmq_params
        tmgr._hb_request_q = 'test_tmgr-hb-request'
        tmgr._hb_response_q = 'test_tmgr-hb-response'

        tmgr._log = mocked_Logger
        tmgr._prof = mocked_Profiler
        tmgr._uid = 'tmgr.0000'
        tmgr.check_heartbeat = mock.MagicMock(return_value=True)
        tmgr.check_manager = mock.MagicMock(return_value=True)

        tmgr._hb_terminate = mt.Event()

        tmgr._hb_thread = mt.Thread(target=_tmgr_side_effect,
                                       name='test_tmgr',
                                       args=(tmgr._hb_terminate,))
        tmgr._hb_thread.start()

        tmgr.terminate_heartbeat()

        self.assertIsNone(tmgr._hb_thread)
