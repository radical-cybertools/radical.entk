# pylint: disable=protected-access,unused-argument,no-value-for-parameter

__copyright__ = 'Copyright 2020-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

from radical.entk.task import Task
from radical.entk import states, exceptions as ree

from unittest import mock, TestCase


# ------------------------------------------------------------------------------
#
class TestTask(TestCase):

    # --------------------------------------------------------------------------
    #
    def test_task_initialization(self):

        t = Task()

        self.assertTrue(t.uid.startswith('task.'))
        self.assertEqual(t.name, '')
        self.assertIsNone(t.rts_uid)
        self.assertEqual(t.state, states.INITIAL)
        self.assertEqual(t.state_history, [states.INITIAL])
        self.assertEqual(t.executable, '')
        self.assertIsInstance(t.arguments, list)
        self.assertIsInstance(t.pre_exec, list)
        self.assertIsInstance(t.post_exec, list)

        self.assertEqual(t.cpu_reqs['cpu_processes'], 1)
        self.assertIsNone(t.cpu_reqs['cpu_process_type'])
        self.assertEqual(t.cpu_reqs['cpu_threads'], 1)
        self.assertIsNone(t.cpu_reqs['cpu_thread_type'])
        self.assertEqual(t.gpu_reqs['gpu_processes'], 0)
        self.assertIsNone(t.gpu_reqs['gpu_process_type'])
        self.assertEqual(t.gpu_reqs['gpu_threads'], 1)
        self.assertIsNone(t.gpu_reqs['gpu_thread_type'])

        self.assertEqual(t.lfs_per_process, 0)
        self.assertEqual(t.sandbox, '')
        self.assertIsInstance(t.upload_input_data, list)
        self.assertIsInstance(t.copy_input_data, list)
        self.assertIsInstance(t.link_input_data, list)
        self.assertIsInstance(t.move_input_data, list)
        self.assertIsInstance(t.copy_output_data, list)
        self.assertIsInstance(t.link_output_data, list)
        self.assertIsInstance(t.move_output_data, list)
        self.assertIsInstance(t.download_output_data, list)
        self.assertFalse(t.stage_on_error)
        self.assertEqual(t.stdout, '')
        self.assertEqual(t.stderr, '')
        self.assertIsNone(t.exit_code)
        self.assertIsNone(t.tags)
        self.assertEqual(t.path, '')
        self.assertIsNone(t.parent_pipeline['uid'])
        self.assertIsNone(t.parent_pipeline['name'])
        self.assertIsNone(t.parent_stage['name'])
        self.assertIsNone(t.parent_stage['uid'])

    # --------------------------------------------------------------------------
    #
    def test_cpu_reqs(self):

        cpu_reqs = {'cpu_processes'   : 2,
                    'cpu_process_type': None,
                    'cpu_threads'     : 1,
                    'cpu_thread_type' : 'OpenMP'}

        task = Task(from_dict={'cpu_reqs': cpu_reqs})

        self.assertEqual(Task.demunch(task.cpu_reqs), cpu_reqs)

        with self.assertRaises(TypeError):
            task.cpu_reqs = list()

        with self.assertRaises(TypeError):
            task.cpu_reqs = {'cpu_processes'   : 'a',
                             'cpu_process_type': None,
                             'cpu_threads'     : 1,
                             'cpu_thread_type' : 'OpenMP'}

        with self.assertRaises(TypeError):
            task.cpu_reqs = {'cpu_processes'   : 1,
                             'cpu_process_type': None,
                             'cpu_threads'     : 'a',
                             'cpu_thread_type' : 'OpenMP'}

    # --------------------------------------------------------------------------
    #
    def test_gpu_reqs(self):

        gpu_reqs = {'gpu_processes'   : 2,
                    'gpu_process_type': None,
                    'gpu_threads'     : 1,
                    'gpu_thread_type' : 'OpenMP'}

        task = Task(from_dict={'gpu_reqs': gpu_reqs})

        self.assertEqual(Task.demunch(task.gpu_reqs), gpu_reqs)

        with self.assertRaises(TypeError):
            task.gpu_reqs = list()

        with self.assertRaises(TypeError):
            task.gpu_reqs = {'gpu_processes'   : 'a',
                             'gpu_process_type': None,
                             'gpu_threads'     : 1,
                             'gpu_thread_type' : 'OpenMP'}

        with self.assertRaises(TypeError):
            task.gpu_reqs = {'gpu_processes'   : 1,
                             'gpu_process_type': None,
                             'gpu_threads'     : 'a',
                             'gpu_thread_type' : 'OpenMP'}

    # --------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.generate_id', return_value='initial.uid.0000')
    def test_uid(self, mocked_generate_id):

        task = Task()

        self.assertEqual(task.uid, 'initial.uid.0000')

        task.uid = 'test.0000'
        self.assertEqual(task.uid, 'test.0000')

        task.uid = 'test.0001'
        self.assertEqual(task.uid, 'test.0001')

        with self.assertRaises(TypeError):
            task.uid = 1

        with self.assertRaises(ree.EnTKError):
            task.uid = 'test:0000'

    # --------------------------------------------------------------------------
    #
    def test_luid(self):

        task = Task()
        task.name            = 'test.0000'
        task.parent_pipeline = {'name': 'p0'}
        task.parent_stage    = {'name': 's0'}
        self.assertEqual(task.luid, 'p0.s0.test.0000')

        task = Task()
        task.uid             = 'test.0001'
        task.parent_pipeline = {'uid': 'p1'}
        task.parent_stage    = {'uid': 's1'}
        self.assertEqual(task.luid, 'p1.s1.test.0001')

    # --------------------------------------------------------------------------
    #
    def test_dict_to_task(self):

        input_dict = {
            'name'      : 'foo',
            'pre_exec'  : ['bar'],
            'executable': 'buz',
            'arguments' : ['baz', 'fiz'],
            'cpu_reqs'  : {'cpu_processes'          : 1,
                           'cpu_process_type'       : None,
                           'cpu_threads': 1,
                           'cpu_thread_type'        : None},
            'gpu_reqs'  : {'gpu_processes'          : 0,
                           'gpu_process_type'       : None,
                           'gpu_threads': 0,
                           'gpu_thread_type'        : None}
        }
        task = Task(from_dict=input_dict)
        for k, v in input_dict.items():
            self.assertEqual(Task.demunch(task[k]), v)

        input_dict = {
            'name'      : 'foo',
            'pre_exec'  : ['bar'],
            'executable': 'buz',
            'arguments' : ['baz', 'fiz'],
            'state'     : states.SUBMITTING,
            'cpu_reqs'  : {'cpu_processes'          : 1,
                            'cpu_process_type'       : None,
                            'cpu_threads': 1,
                            'cpu_thread_type'        : None}}
        task = Task()
        task.from_dict(input_dict)

        for k, v in input_dict.items():
            self.assertEqual(Task.demunch(task[k]), v)

        with self.assertRaises(ree.TypeError):
            Task(from_dict='input_str')

    # --------------------------------------------------------------------------
    #
    def test_executable(self):

        task = Task()
        task.executable = 'test_exec'
        self.assertEqual(task.executable, 'test_exec')

        task = Task()
        task._cast = False  # if True, then `executable` will be "['test_exec']"
        with self.assertRaises(TypeError):
            task.executable = ['test_exec']

    # --------------------------------------------------------------------------
    #
    def test_task_to_dict(self):

        expected_dict = {
            'uid'                 : 'test.0000',
            'name'                : '',
            'state'               : 'DESCRIBED',
            'state_history'       : ['DESCRIBED'],
            'pre_exec'            : [],
            'executable'          : '',
            'arguments'           : [],
            'sandbox'             : '',
            'post_exec'           : [],
            'cpu_reqs'            : {'cpu_processes'   : 1,
                                     'cpu_process_type': 'POSIX',
                                     'cpu_threads'     : 1,
                                     'cpu_thread_type' : 'POSIX'},
            'gpu_reqs'            : {'gpu_processes'   : 0,
                                     'gpu_process_type': 'POSIX',
                                     'gpu_threads'     : 0,
                                     'gpu_thread_type' : 'POSIX'},
            'lfs_per_process'     : 0,
            'upload_input_data'   : [],
            'copy_input_data'     : [],
            'link_input_data'     : [],
            'move_input_data'     : [],
            'copy_output_data'    : [],
            'link_output_data'    : [],
            'move_output_data'    : [],
            'download_output_data': [],
            'stdout'              : 'Hello World',
            'stderr'              : 'Hello World',
            'stage_on_error'      : False,
            'exit_code'           : 0,
            'path'                : 'some_path',
            'tags'                : {'colocate': 'tag.0001'},
            'rts_uid'             : 'task.0013',
            'parent_stage'        : {'uid' : 'stage.0000',
                                     'name': 'stage.0000'},
            'parent_pipeline'     : {'uid' : 'pipe.0000',
                                     'name': 'pipe.0000'}}

        task = Task(from_dict=expected_dict)
        self.assertEqual(task.to_dict(), expected_dict)

    # --------------------------------------------------------------------------
    #
    def test_state_history(self):

        task = Task()

        self.assertEqual(task.state_history, [states.INITIAL])

        task.state = states.SCHEDULED
        self.assertEqual(task.state_history, [states.INITIAL, states.SCHEDULED])

        task.state_history = [states.DONE]
        self.assertEqual(task.state_history, [states.DONE])

        with self.assertRaises(ree.ValueError):
            task.state_history = ['WRONG_STATE_NAME']

# ------------------------------------------------------------------------------
