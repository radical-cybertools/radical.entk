# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter, import-error

import timeout_decorator

import multiprocessing       as mp
import threading             as mt
import hypothesis.strategies as st

from unittest   import mock, TestCase
from hypothesis import given, settings

import radical.utils           as ru
import radical.entk            as re
import radical.entk.exceptions as ree

from radical.entk import AppManager as Amgr


# ------------------------------------------------------------------------------
#
class TestBase(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Amgr, '__init__', return_value=None)
    @given(d=st.fixed_dictionaries({'reattempts'     : st.integers(),
                                    'resubmit_failed': st.booleans(),
                                    'autoterminate'  : st.booleans(),
                                    'write_workflow' : st.booleans()}))
    @settings(max_examples=10)
    def test_amgr_read_config(self, d, mocked_init):

        amgr = Amgr()
        amgr._rmgr     = None
        amgr._services = []

        d['rts']        = 'mock'
        d['rts_config'] = {'sandbox_cleanup': True,
                           'db_cleanup'     : True}

        ru.write_json(d, './config.json')
        amgr._read_config(config_path='./',
                          reattempts=None,
                          resubmit_failed=None,
                          autoterminate=None,
                          write_workflow=None,
                          rts=None,
                          rts_config=None,
                          base_path=None)

        self.assertEqual(amgr._reattempts,      d['reattempts'])
        self.assertEqual(amgr._resubmit_failed, d['resubmit_failed'])
        self.assertEqual(amgr._autoterminate,   d['autoterminate'])
        self.assertEqual(amgr._write_workflow,  d['write_workflow'])
        self.assertEqual(amgr._rts,             d['rts'])
        self.assertEqual(amgr._rts_config,      d['rts_config'])

        self.assertEqual(amgr.services, [])

        d['rts'] = 'another'
        ru.write_json(d, './config.json')
        with self.assertRaises(ValueError):
            # not supported RTS
            amgr._read_config(config_path='./',
                              reattempts=None,
                              resubmit_failed=None,
                              autoterminate=None,
                              write_workflow=None,
                              rts=None,
                              rts_config=None,
                              base_path=None)

        # test configuring services (service tasks)

        with self.assertRaises(ree.EnTKTypeError):
            # only re.Task objects are acceptable
            amgr.services = [None]

        services = [re.Task()]
        amgr.services = services
        # without initialized ResourceManager
        self.assertEqual(amgr._services, services)

        amgr._rmgr = mock.Mock()
        amgr.services = services
        # with initialized ResourceManager
        self.assertEqual(amgr._rmgr.services, services)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Amgr, '__init__', return_value=None)
    @given(d2=st.fixed_dictionaries({'reattempts'     : st.integers(),
                                     'resubmit_failed': st.booleans(),
                                     'autoterminate'  : st.booleans(),
                                     'write_workflow' : st.booleans()}))
    @settings(max_examples=10)
    def test_amgr_read_config2(self, mocked_init, d2):

        services_mocked = [mock.Mock()]

        amgr = Amgr()
        amgr._rmgr          = mock.Mock()
        amgr._rmgr.services = services_mocked

        amgr._read_config(config_path='./',
                          reattempts=d2['reattempts'],
                          resubmit_failed=d2['resubmit_failed'],
                          autoterminate=d2['autoterminate'],
                          write_workflow=d2['write_workflow'],
                          rts='mock',
                          rts_config={'sandbox_cleanup': True,
                                      'db_cleanup'     : True},
                          base_path=None)

        self.assertEqual(amgr._reattempts,      d2['reattempts'])
        self.assertEqual(amgr._resubmit_failed, d2['resubmit_failed'])
        self.assertEqual(amgr._autoterminate,   d2['autoterminate'])
        self.assertEqual(amgr._write_workflow,  d2['write_workflow'])
        self.assertEqual(amgr._rts,             'mock')
        self.assertEqual(amgr._rts_config,      {'sandbox_cleanup': True,
                                                 'db_cleanup'     : True})

        self.assertEqual(amgr.services, services_mocked)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Amgr, '_setup_zmq', return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    @mock.patch('radical.utils.Reporter')
    def test_amgr_deprecated_args(self, mocked_reporter, mocked_profiler,
                                  mocked_logger, mocked_setup_zmq):

        with self.assertWarns(DeprecationWarning) as dw:

            Amgr(name='amgr.test',
                 hostname='deprecated_rmq_hostname',
                 port='deprecated_rmq_port',
                 username='deprecated_rmq_username',
                 password='deprecated_rmq_password')

            self.assertEqual(len(dw.warnings), 4)  # 4 deprecated arguments

            for w in dw.warnings:
                self.assertIs(w.category, DeprecationWarning)
                warning_msg = str(w.message)
                self.assertTrue(any(
                    warning_msg.startswith(a)
                    for a in ['hostname', 'port', 'username', 'password']))
                self.assertTrue(warning_msg.endswith('is not required anymore'))

    # --------------------------------------------------------------------------
    #
    @timeout_decorator.timeout(5)
    @mock.patch.object(Amgr, '__init__', return_value=None)
    @mock.patch.object(Amgr, '_submit_rts_tmgr', return_value=True)
    @mock.patch('radical.entk.execman.mock.ResourceManager',
                return_value=mock.MagicMock(
                    get_resource_allocation_state=mock.MagicMock(
                        side_effect=['RUNNING', 'DONE', 'RUNNING', 'RUNNING',
                                     'RUNNING', 'RUNNING']),
                    get_completed_states=mock.MagicMock(return_value=['DONE']),
                    get_rts_info=mock.MagicMock(
                        return_value={'pilot': 'pilot.0000'})))
    @mock.patch('radical.entk.execman.mock.TaskManager',
                return_value=mock.MagicMock(
                    terminate_manager=mock.MagicMock(return_value=True),
                    start_manager=mock.MagicMock(return_value=True)))
    @mock.patch('radical.entk.appman.wfprocessor.WFprocessor',
                return_value=mock.MagicMock(
                    workflow_incomplete=mock.MagicMock(return_value=True),
                    check_processor=mock.MagicMock(
                        side_effect=[True, False, False, False, False, True]),
                    terminate_processor=mock.MagicMock(return_value=True),
                    start_processor=mock.MagicMock(return_value=True)))
    @mock.patch('radical.utils.Profiler')
    @mock.patch('radical.utils.Logger')
    def test_run_workflow(self, mocked_logger, mocked_profiler,
                          mocked_wfprocessor, mocked_tmgr, mocked_rmgr,
                          mocked_submit_rts_tmgr, mocked_init):

        appman = Amgr()
        appman._uid          = 'appman.0000'
        appman._wfp          = re.appman.wfprocessor.WFprocessor()
        appman._task_manager = re.execman.mock.TaskManager(None, None, None)
        appman._rmgr         = re.execman.mock.ResourceManager(None, None, None)
        appman._logger       = mocked_logger
        appman._prof         = mocked_profiler
        appman._term         = mp.Event()
        pipe                 = mock.Mock()
        pipe.lock            = mt.Lock()
        pipe.completed       = False
        pipe.uid             = 'pipe.0000'
        appman._workflow     = {pipe}
        appman._cur_attempt  = 1
        appman._reattempts   = 3
        appman._sync_thread  = mock.Mock()
        appman._sync_thread.is_alive = mock.MagicMock(return_value=True)

        with self.assertRaises(ree.EnTKError):
            appman._run_workflow()

        self.assertEqual(appman._cur_attempt, 4)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Amgr, '__init__', return_value=None)
    def test_update_task(self, mocked_init):

        task             = mock.Mock()
        task.uid         = 'task.0000'
        task.luid        = 't0.s0.p0'
        task.state       = re.states.SUBMITTING

        stage            = mock.Mock()
        stage.uid        = 'stage.0000'
        stage.tasks      = {task}

        pipe             = mock.Mock()
        pipe.uid         = 'pipe.0000'
        pipe.lock        = mt.Lock()
        pipe.completed   = False
        pipe.stages      = [stage]

        appman           = Amgr()
        appman._uid      = 'appman.0000'
        appman._workflow = [pipe]
        appman._logger = appman._prof = appman._report = mock.Mock()

        msg = {'uid'            : task.uid,
               'state'          : re.states.COMPLETED,
               'parent_stage'   : {'uid': stage.uid},
               'parent_pipeline': {'uid': pipe.uid}}

        # confirm that `task` has different state than `msg` contains
        self.assertNotEqual(task.state, msg['state'])

        # task will be "found" and method below will be called
        # `mq_channel.basic_ack(delivery_tag=method_frame.delivery_tag)`
        appman._update_task(msg)

# ------------------------------------------------------------------------------

