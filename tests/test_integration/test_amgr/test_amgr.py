# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

from unittest   import TestCase
import threading as mt
import os

try:
    import mock
except ImportError:
    from unittest import mock

# import radical.utils as ru
import radical.entk.exceptions as ree

from radical.entk import AppManager as Amgr
import radical.entk as re


class TestBase(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Amgr, '_submit_rts_tmgr', return_value=True)
    @mock.patch('radical.entk.appman.wfprocessor.WFprocessor',
                return_value=mock.MagicMock(workflow_incomplete=mock.MagicMock(return_value=True),
                                            check_processor=mock.MagicMock(return_value=True),
                                            terminate_processor=mock.MagicMock(return_value=True),
                                            start_processor=mock.MagicMock(return_value=True)
                                            ))
    @mock.patch('radical.utils.Profiler')
    @mock.patch('radical.utils.Logger')
    def test_run_workflow(self, mocked_submit_rts_tmgr,
                          mocked_WFprocessor, mocked_Profiler,
                          mocked_Logger):
        os.environ['RU_RAISE_ON_SYNC_FAIL'] = '3'
        os.environ['RU_RAISE_ON_RESOURCE_FAIL'] = '15'
        os.environ['RU_RAISE_ON_TMGR_FAIL'] = '10'
        appman = Amgr(rts='mock')
        appman._wfp = re.appman.wfprocessor.WFprocessor()
        appman._rmgr = re.execman.mock.ResourceManager(resource_desc={},
                                                sid='test_rmgr',rts_config=None)
        re.execman.mock.TaskManager._setup_zmq = lambda x, y: True
        appman._task_manager = re.execman.mock.TaskManager(sid='test_tmgr',
                                                 rmgr=appman._rmgr, zmq_info={})
        appman._uid = 'appman.0000'
        appman._logger = mocked_Logger
        appman._prof = mocked_Profiler
        appman._terminate_sync = mt.Event()
        pipe = mock.Mock()
        pipe.lock = mt.Lock()
        pipe.completed = False
        pipe.uid = 'pipe.0000'
        appman._workflow = set([pipe])
        appman._cur_attempt = 1
        appman._reattempts = 3
        appman._sync_thread = mt.Thread(target=appman._synchronizer,
                                          name='synchronizer-thread')
        appman._sync_thread.start()
        with self.assertRaises(ree.EnTKError):
            appman._run_workflow()

        self.assertEqual(appman._cur_attempt, 4)
        appman._task_manager.terminate_manager()
        appman._terminate_sync.set()
        appman._sync_thread.join()

