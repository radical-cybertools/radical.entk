# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

from unittest import TestCase

from radical.entk.execman.rp   import TaskManager as RPTmgr
from radical.entk.execman.rp   import ResourceManager as RPRmgr

import pika
from pika.connection import ConnectionParameters

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
        rmgr = mock.MagicMock(spec=RPRmgr)
        tmgr = RPTmgr('test_tmgr', ['pending_queues'], ['completed_queues'], 
                     rmgr, rmq_params)
        self.assertIsNone(tmgr._rts_runner)
        self.assertEqual(tmgr._rmq_ping_interval, 10)
