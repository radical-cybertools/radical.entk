# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

from unittest import TestCase

# from hypothesis import given, settings, strategies as st
import threading as mt
import time

from radical.entk.appman.wfprocessor import WFprocessor
from radical.entk                    import states

try:
    import mock
except ImportError:
    from unittest import mock

# Hypothesis settings
# settings.register_profile("travis", max_examples=100, deadline=None)
# settings.load_profile("travis")


# ------------------------------------------------------------------------------
#
class TestBase(TestCase):

    @mock.patch('radical.utils.generate_id', return_value='wfp.0000')
    @mock.patch('os.getcwd', return_value='test_folder')
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    @mock.patch('radical.utils.Reporter')
    def test_wfp_initialization(self, mocked_generate_id, mocked_getcwd,
                                mocked_Logger, mocked_Profiler, mocked_Reporter):

        wfp = WFprocessor._setup_zmq = lambda x, y: True
        wfp = WFprocessor(sid='test_sid', workflow='workflow',
                resubmit_failed=False, zmq_info={})

        self.assertIsNone(wfp._wfp_process)
        self.assertIsNone(wfp._enqueue_thread)
        self.assertIsNone(wfp._dequeue_thread)
        self.assertIsNone(wfp._enqueue_thread_terminate)
        self.assertIsNone(wfp._dequeue_thread_terminate)
        self.assertEqual(wfp._path, 'test_folder/test_sid')
        self.assertEqual(wfp._workflow, 'workflow')


        self.assertEqual(wfp._sid, 'test_sid')
        self.assertFalse(wfp._resubmit_failed)
        self.assertEqual(wfp._uid, 'wfp.0000')

        wfp = WFprocessor(sid='test_sid', workflow='workflow',
                resubmit_failed=True, zmq_info={})

        self.assertIsNone(wfp._wfp_process)
        self.assertIsNone(wfp._enqueue_thread)
        self.assertIsNone(wfp._dequeue_thread)
        self.assertEqual(wfp._path, 'test_folder/test_sid')
        self.assertEqual(wfp._workflow, 'workflow')
        self.assertEqual(wfp._sid, 'test_sid')
        self.assertTrue(wfp._resubmit_failed)
        self.assertEqual(wfp._uid, 'wfp.0000')

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(WFprocessor, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_workflow(self, mocked_init, mocked_Logger):

        wfp = WFprocessor(sid='test_sid', workflow='workflow',
                resubmit_failed=False, zmq_info={})

        wfp._workflow = 'test_workflow'

        self.assertEqual(wfp.workflow, 'test_workflow')

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(WFprocessor, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_wfp_workflow_incomplete(self, mocked_init, mocked_Logger):

        wfp = WFprocessor(sid='test_sid', workflow='workflow',
                resubmit_failed=False, zmq_info={})
        wfp._logger = mocked_Logger
        pipe = mock.Mock()
        pipe.lock = mt.Lock()
        pipe.completed = False
        wfp._workflow = set([pipe])
        self.assertTrue(wfp.workflow_incomplete())

        pipe.completed = True
        self.assertFalse(wfp.workflow_incomplete())

        wfp = WFprocessor(sid='test_sid', workflow='workflow',
                resubmit_failed=False, zmq_info={})
        with self.assertRaises(Exception):
            wfp.workflow_incomplete()

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(WFprocessor, '__init__', return_value=None)
    def test_check_processor(self, mocked_init):

        wfp = WFprocessor(sid='test_sid', workflow='workflow',
                resubmit_failed=False, zmq_info={})

        wfp._enqueue_thread = None
        wfp._dequeue_thread = None

        self.assertFalse(wfp.check_processor())

        wfp._enqueue_thread = mock.Mock()
        wfp._enqueue_thread.is_alive = mock.MagicMock(side_effect=[False, False,
                                                                   True, True])
        wfp._dequeue_thread = mock.Mock()
        wfp._dequeue_thread.is_alive = mock.MagicMock(side_effect=[False, True,
                                                                   False, True])

        self.assertFalse(wfp.check_processor())
        self.assertFalse(wfp.check_processor())
        self.assertFalse(wfp.check_processor())
        self.assertTrue(wfp.check_processor())


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(WFprocessor, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    @mock.patch('time.sleep', return_value=None)
    @mock.patch.object(WFprocessor, '_create_workload',
                       side_effect=[['task','stages'],[]])
    @mock.patch.object(WFprocessor, '_execute_workload', retur_value=True)
    def test_enqueue(self, mocked_init, mocked_Logger, mocked_Profiler,
                     mocked_sleep, mocked_create_workload, mocked_execute_workload):
        wfp = WFprocessor(sid='test_sid', workflow='workflow',
                resubmit_failed=False, zmq_info={})
        wfp._logger = mocked_Logger
        wfp._prof = mocked_Profiler
        wfp._uid = 'wfp.0000'
        wfp._enqueue_thread_terminate = mock.Mock()
        wfp._enqueue_thread_terminate.is_set = mock.MagicMock(side_effect=[False, True])

        wfp._enqueue()

        with self.assertRaises(Exception):
            wfp._enqueue()

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(WFprocessor, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Reporter')
    def test_advance(self, mocked_init, mocked_Logger, mocked_Reporter):
        wfp = WFprocessor(sid='test_sid', workflow='workflow',
                resubmit_failed=False, zmq_info={})

        global_profs = []

        def _log(log_entry, uid, state, msg):
            nonlocal global_profs
            global_profs.append([log_entry, uid, state, msg])

        wfp._logger = mocked_Logger
        wfp._report = mocked_Reporter
        wfp._prof = mock.Mock()
        wfp._prof.prof = mock.MagicMock(side_effect=_log)
        wfp._uid = 'wfp.0000'
        obj = mock.Mock()
        obj.parent_stage = {'uid': 'test_stage'}
        obj.parent_pipeline = {'uid': 'test_pipe'}
        obj.uid = 'test_object'
        obj.state = 'test_state'
        wfp._advance(obj, 'Task', None)
        self.assertEqual(global_profs[0],['advance', 'test_object', None, 'test_stage'])
        global_profs = []
        wfp._advance(obj, 'Stage', 'new_state')
        self.assertEqual(global_profs[0],['advance','test_object', 'new_state', 'test_pipe'])
        global_profs = []
        wfp._advance(obj, 'Pipe', 'new_state')
        self.assertEqual(global_profs[0],['advance','test_object', 'new_state', None])


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(WFprocessor, '__init__', return_value=None)
    @mock.patch.object(WFprocessor, '_advance', return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Reporter')
    def test_create_workload(self, mocked_init, mocked_advance, mocked_Logger,
                             mocked_Reporter):
        wfp = WFprocessor(sid='test_sid', workflow='workflow',
                resubmit_failed=False, zmq_info={})

        wfp._resubmit_failed = False
        pipe = mock.Mock()
        pipe.lock = mt.Lock()
        pipe.state = states.INITIAL
        pipe.completed = False
        pipe.current_stage = 1

        stage = mock.Mock()
        stage.uid = 'stage.0000'
        stage.state = states.SCHEDULING

        task = mock.Mock()
        task.uid = 'task.0000'
        task.state = states.INITIAL

        stage.tasks = [task]
        pipe.stages = [stage]
        wfp._workflow = set([pipe])

        workload, scheduled_stages = wfp._create_workload()

        self.assertEqual(workload, [task])
        self.assertEqual(scheduled_stages, [stage])


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(WFprocessor, '__init__', return_value=None)
    @mock.patch('json.dumps', return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Reporter')
    def test_execute_workload(self, mocked_init, mocked_dumps,
                              mocked_Logger, mocked_Reporter):

        global_advs = []

        def _advance_side_effect(obj, obj_type, state):
            nonlocal global_advs
            global_advs.append([obj, obj_type, state])

        wfp = WFprocessor(sid='test_sid', workflow='workflow',
                resubmit_failed=False, zmq_info={})
        wfp._logger = mocked_Logger
        wfp._advance = mock.MagicMock(side_effect=_advance_side_effect)
        wfp._zmq_queue = mock.MagicMock()

        stage = mock.Mock()
        stage.uid = 'stage.0000'
        stage.state = states.SCHEDULING

        task = mock.Mock()
        task.uid = 'task.0000'
        task.state = states.INITIAL
        workload = [task]
        stage.tasks = [task]
        scheduled_stages = [stage]


        wfp._execute_workload(workload, scheduled_stages)
        self.assertEqual(global_advs[0], [task, 'Task', 'SCHEDULED'])
        self.assertEqual(global_advs[1], [stage, 'Stage', 'SCHEDULED'])

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(WFprocessor, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_reset_workload(self, mocked_init, mocked_Logger):

        global_advs = []

        def _advance_side_effect(obj, obj_type, state):
            nonlocal global_advs
            global_advs.append([obj, obj_type, state])

        wfp = WFprocessor(sid='test_sid', workflow='workflow',
                          resubmit_failed=False, zmq_info={})
        wfp._logger = mocked_Logger
        wfp._advance = mock.MagicMock(side_effect=_advance_side_effect)

        pipe = mock.Mock()
        pipe.lock = mt.Lock()
        pipe.state = states.SCHEDULING
        pipe.completed = False
        pipe.current_stage = 1

        stage = mock.Mock()
        stage.uid = 'stage.0000'
        stage.state = states.SCHEDULED

        task = mock.Mock()
        task.uid = 'task.0000'
        task.state = states.SCHEDULED

        stage.tasks = [task]
        pipe.stages = [stage]

        pipe2 = mock.Mock()
        pipe2.lock = mt.Lock()
        pipe2.state = states.DONE
        pipe2.uid = 'pipe.0001'
        pipe2.completed = True

        pipe3 = mock.Mock()
        pipe3.lock = mt.Lock()
        pipe3.state = states.SUSPENDED
        pipe3.uid = 'pipe.0002'
        pipe3.completed = False

        wfp._workflow = set([pipe, pipe2, pipe3])

        wfp.reset_workflow()
        self.assertEqual(global_advs[0], [task, 'Task', 'DESCRIBED'])
        self.assertEqual(global_advs[1], [stage, 'Stage', 'SCHEDULING'])

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(WFprocessor, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    def test_execute_post_exec(self, mocked_init,
                              mocked_Logger, mocked_Profiler):

        global_advs = set()

        def _advance_side_effect(obj, obj_type, state):
            nonlocal global_advs
            global_advs.add((obj, obj_type, state))

        wfp = WFprocessor(sid='test_sid', workflow='workflow',
                resubmit_failed=False, zmq_info={})
        wfp._uid = 'wfp.0000'
        wfp._logger = mocked_Logger
        wfp._prof = mocked_Profiler
        wfp._advance = mock.MagicMock(side_effect=_advance_side_effect)

        pipe = mock.Mock()
        pipe.lock = mt.Lock()
        pipe.state = states.INITIAL
        pipe.uid = 'pipe.0000'
        pipe.completed = False
        pipe._increment_stage = mock.MagicMock(return_value=True)
        pipe.current_stage = 1

        pipe2 = mock.Mock()
        pipe2.lock = mt.Lock()
        pipe2.state = states.INITIAL
        pipe2.uid = 'pipe.0001'
        pipe2.completed = False
        pipe2._increment_stage = mock.MagicMock(return_value=True)
        pipe2.current_stage = 1

        pipe3 = mock.Mock()
        pipe3.lock = mt.Lock()
        pipe3.state = states.INITIAL
        pipe3.uid = 'pipe.0002'
        pipe3.completed = True
        pipe3._increment_stage = mock.MagicMock(return_value=True)
        pipe3.current_stage = 1
        wfp._workflow = set([pipe2, pipe3])

        stage = mock.Mock()
        stage.uid = 'stage.0000'
        stage.state = states.SCHEDULING
        stage.post_exec = mock.MagicMock(return_value=['pipe.0001', 'pipe.0002'])

        wfp._execute_post_exec(pipe, stage)
        exp_out = set([(pipe2, 'Pipeline', states.INITIAL),
                       (pipe3, 'Pipeline', states.DONE)])
        self.assertEqual(global_advs, exp_out)

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(WFprocessor, '__init__', return_value=None)
    @mock.patch.object(WFprocessor, '_advance', return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    def test_update_dequeued_task(self, mocked_init, mocked_advance, mocked_Logger,
                             mocked_Profiler):
        global_advs = list()

        def _advance_side_effect(obj, obj_type, state):
            nonlocal global_advs
            global_advs.append([obj, obj_type, state])

        wfp = WFprocessor(sid='test_sid', workflow='workflow',
                resubmit_failed=False, zmq_info={})

        wfp._uid = 'wfp.0000'
        wfp._logger = mocked_Logger
        wfp._prof = mocked_Profiler
        wfp._resubmit_failed = False
        wfp._advance = mock.MagicMock(side_effect=_advance_side_effect)

        pipe = mock.Mock()
        pipe.uid = 'pipe.0000'
        pipe.lock = mt.Lock()
        pipe.state = states.INITIAL
        pipe.completed = False
        pipe.current_stage = 1
        pipe._increment_stage = mock.MagicMock(return_value=2)

        stage = mock.Mock()
        stage.uid = 'stage.0000'
        stage.state = states.SCHEDULING
        stage._check_stage_complete = mock.MagicMock(return_value=True)
        stage.post_exec = None

        task = mock.Mock()
        task.uid = 'task.0000'
        task.parent_pipeline = {'uid': 'pipe.0000'}
        task.parent_stage = {'uid': 'stage.0000'}
        task.state = states.INITIAL
        task.exit_code = 0

        stage.tasks = [task]
        pipe.stages = [stage]
        wfp._workflow = set([pipe])

        # Test for issue #271
        wfp._update_dequeued_task(task)
        self.assertEqual(global_advs[0], [task, 'Task', states.DONE])
        self.assertEqual(global_advs[1], [stage, 'Stage', states.DONE])

        task.state = states.INITIAL
        task.exit_code = None

        wfp._update_dequeued_task(task)
        self.assertEqual(global_advs[2], [task, 'Task', states.INITIAL])
        self.assertEqual(global_advs[3], [stage, 'Stage', states.DONE])

        task.exit_code = 1

        wfp._update_dequeued_task(task)
        self.assertEqual(global_advs[4], [task, 'Task', states.FAILED])
        self.assertEqual(global_advs[5], [stage, 'Stage', states.DONE])

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(WFprocessor, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    def test_start_processor(self, mocked_init, mocked_Logger,
                             mocked_Profiler):

        global_boolean = {}

        def _dequeue_side_effect():
            nonlocal global_boolean
            global_boolean['dequeue'] = True


        def _enqueue_side_effect():
            nonlocal global_boolean
            global_boolean['enqueue'] = True

        wfp = WFprocessor(sid='test_sid', workflow='workflow',
                resubmit_failed=False, zmq_info={})

        wfp._uid = 'wfp.0000'
        wfp._logger = mocked_Logger
        wfp._prof = mocked_Profiler
        wfp._enqueue_thread = None
        wfp._dequeue_thread = None
        wfp._enqueue_thread_terminate = None
        wfp._dequeue_thread_terminate = None
        wfp._dequeue = mock.MagicMock(side_effect=_dequeue_side_effect)
        wfp._enqueue = mock.MagicMock(side_effect=_enqueue_side_effect)

        wfp.start_processor()
        time.sleep(1)
        try:
            self.assertIsInstance(wfp._enqueue_thread_terminate, mt.Event)
            self.assertIsInstance(wfp._dequeue_thread_terminate, mt.Event)
            self.assertIsInstance(wfp._dequeue_thread, mt.Thread)
            self.assertIsInstance(wfp._enqueue_thread, mt.Thread)
            self.assertTrue(global_boolean['dequeue'])
            self.assertTrue(global_boolean['enqueue'])
        finally:
            if wfp._dequeue_thread.is_alive():
                wfp._dequeue_thread.join()
            if wfp._enqueue_thread.is_alive():
                wfp._enqueue_thread.join()


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(WFprocessor, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    @mock.patch('radical.utils.Profiler')
    def test_terminate_processor(self, mocked_init, mocked_Logger,
                                 mocked_Profiler):

        global_boolean = {}

        def _dequeue_side_effect():
            nonlocal global_boolean
            time.sleep(0.1)
            global_boolean['dequeue'] = True


        def _enqueue_side_effect():
            nonlocal global_boolean
            time.sleep(0.1)
            global_boolean['enqueue'] = True

        wfp = WFprocessor(sid='test_sid', workflow='workflow',
                resubmit_failed=False, zmq_info={})

        wfp._uid = 'wfp.0000'
        wfp._logger = mocked_Logger
        wfp._prof = mocked_Profiler
        wfp._enqueue_thread = mt.Thread(target=_enqueue_side_effect)
        wfp._dequeue_thread = mt.Thread(target=_dequeue_side_effect)
        wfp._enqueue_thread_terminate = mt.Event()
        wfp._dequeue_thread_terminate = mt.Event()


        wfp._enqueue_thread.start()
        wfp._dequeue_thread.start()

        wfp.terminate_processor()

        self.assertTrue(wfp._enqueue_thread_terminate.is_set())
        self.assertTrue(wfp._dequeue_thread_terminate.is_set())
        self.assertIsNone(wfp._dequeue_thread)
        self.assertIsNone(wfp._enqueue_thread)
        self.assertTrue(global_boolean['dequeue'])
        self.assertTrue(global_boolean['enqueue'])
