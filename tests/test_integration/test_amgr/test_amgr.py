# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

from unittest   import TestCase
from hypothesis import given, settings
import threading as mt
import timeout_decorator
import os

try:
    import mock
except ImportError:
    from unittest import mock

import radical.utils as ru
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
    @mock.patch('radical.entk.execman.mock.TaskManager',
                return_value=mock.MagicMock(check_heartbeat=mock.MagicMock(return_value=True),
                                            terminate_heartbeat=mock.MagicMock(return_value=True),
                                            terminate_manager=mock.MagicMock(return_value=True),
                                            start_manager=mock.MagicMock(return_value=True),
                                            start_heartbeat=mock.MagicMock(return_value=True)
                                            ))
    @mock.patch('radical.utils.Profiler')
    @mock.patch('radical.utils.Logger')
    def test_run_workflow(self, mocked_submit_rts_tmgr,
                          mocked_WFprocessor, mocked_TaskManager, mocked_Profiler,
                          mocked_Logger):
        os.environ['RU_RAISE_ON_SYNC_FAIL']='3'
        os.environ['RU_RAISE_ON_RESOURCE_FAIL']='15'
        os.environ['RU_RAISE_ON_TMGR_FAIL']='10'
        hostname = os.environ.get('RMQ_HOSTNAME', 'localhost')
        port = int(os.environ.get('RMQ_PORT', '5672'))
        username = os.environ.get('RMQ_USERNAME')
        password = os.environ.get('RMQ_PASSWORD')
        appman = Amgr(hostname=hostname, port=port, username=username,
            password=password)
        appman._wfp = re.appman.wfprocessor.WFprocessor()
        appman._task_manager = re.execman.mock.TaskManager()
        appman._rmgr = re.execman.mock.ResourceManager(resource_desc={}, sid='test',rts_config=None)
        appman._uid = 'appman.0000'
        appman._logger = mocked_Logger
        appman._prof = mocked_Profiler
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
