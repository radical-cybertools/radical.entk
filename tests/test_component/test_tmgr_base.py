# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter, import-error

from unittest import TestCase

from radical.entk import Task

from radical.entk.execman.base.task_manager import EnTKError

from radical.entk.execman.base   import Base_TaskManager as Tmgr
from radical.entk.execman.base   import Base_ResourceManager

import time
import threading as mt

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
    def test_init(self, mocked_generate_id, mocked_getcwd, mocked_Logger,
                  mocked_Profiler, mocked_DebugHelper):

        rmgr = mock.MagicMock(spec=Base_ResourceManager)
        Tmgr._setup_zmq = lambda x, y: True
        tmgr = Tmgr('test_tmgr', rmgr, 'test_rts', {})

        self.assertEqual(tmgr._sid, 'test_tmgr')
        self.assertEqual(tmgr._rmgr, rmgr)
        self.assertEqual(tmgr._rts, 'test_rts')
        self.assertEqual(tmgr._uid, 'tmgr.0000')
        self.assertEqual(tmgr._path, 'test_folder/test_tmgr')
        self.assertIsNone(tmgr._tmgr_thread)
        self.assertIsNone(tmgr._tmgr_terminate)

        with self.assertRaises(NotImplementedError):
            # method should be overloaded
            tmgr._tmgr(None, None, None)

        with self.assertRaises(NotImplementedError):
            # method should be overloaded
            tmgr.start_manager()

        with self.assertRaises(TypeError):
            tmgr = Tmgr(25, rmgr, 'test_rts', {})

        with self.assertRaises(TypeError):
            tmgr = Tmgr('test_tmgr', rmgr, 'test_rts', [])


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Tmgr, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    def test_advance(self, mocked_init, mocked_Logger, mocked_Profiler):

        rmgr = mock.MagicMock(spec=Base_ResourceManager)
        tmgr = Tmgr('test_tmgr', rmgr, 'test_rts', {})

        global_syncs = []

        def _sync_side_effect(obj, obj_type, channel):
            nonlocal global_syncs
            global_syncs.append([obj, obj_type, channel])

        tmgr._log = mocked_Logger
        tmgr._prof = mocked_Profiler
        tmgr._sync_with_master = mock.MagicMock(side_effect=_sync_side_effect)
        tmgr._uid = 'tmgr.0000'
        obj = Task({'uid': 'test.object.00'})
        obj.parent_stage = {'uid': 'test_stage'}
        obj.parent_pipeline = {'uid': 'test_pipe'}

        tmgr._advance(obj, 'Task', 'SCHEDULING', 'channel')
        self.assertEqual(global_syncs[0],
                         [obj, 'Task', 'channel'])
        self.assertEqual(obj.state, 'SCHEDULING')

        # no `obj` type - will not change the processing flow
        tmgr._advance(obj, 'unknown_type', None, 'channel')
        self.assertIsNone(obj.state)

        global_syncs = []
        tmgr._advance(obj, 'Stage', 'DONE', 'channel')
        self.assertEqual(global_syncs[0],
                         [obj, 'Stage', 'channel'])
        self.assertEqual(obj.state, 'DONE')

        # mimic exception
        tmgr._prof.prof = mock.MagicMock(side_effect=Exception('error'))
        with self.assertRaises(EnTKError):
            # whenever Exception is raised `EnTKError` is used
            tmgr._advance(obj, None, None, None)


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Tmgr, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    def test_check_manager(self, mocked_init, mocked_Logger, mocked_Profiler):

        rmgr = mock.MagicMock(spec=Base_ResourceManager)
        tmgr = Tmgr('test_tmgr', rmgr, 'test_rts', {})

        def _tmgr_side_effect(amount):
            time.sleep(amount)

        tmgr._tmgr_thread = mt.Thread(target=_tmgr_side_effect,
                                      name='test_tmgr', args=(1,))
        tmgr._tmgr_thread.start()

        self.assertTrue(tmgr.check_manager())
        tmgr._tmgr_thread.join()
        self.assertFalse(tmgr.check_manager())

        tmgr._tmgr_thread = None
        self.assertFalse(tmgr.check_manager())

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Tmgr, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    def test_terminate_manager(self, mocked_init, mocked_Logger, mocked_Profiler):

        rmgr = mock.MagicMock(spec=Base_ResourceManager)
        tmgr = Tmgr('test_tmgr', rmgr, 'test_rts', {})

        tmgr._log = mocked_Logger
        tmgr._prof = mocked_Profiler
        tmgr._uid = 'tmgr.0000'

        tmgr._tmgr_terminate = mt.Event()

        tmgr._tmgr_thread = mt.Thread(target=_tmgr_side_effect,
                                      name='test_tmgr',
                                      args=(tmgr._tmgr_terminate,))
        tmgr._tmgr_thread.start()

        tmgr.check_manager = mock.MagicMock(return_value=True)
        tmgr.terminate_manager()

        self.assertIsNone(tmgr._tmgr_thread)
        if tmgr._tmgr_thread:
            self.assertFalse(tmgr._tmgr_thread.is_alive())

