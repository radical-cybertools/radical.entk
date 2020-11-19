# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

__copyright__ = "Copyright 2020, http://radical.rutgers.edu"
__license__   = "MIT"

from unittest import TestCase

import pytest

from radical.entk.task import Task
from radical.entk import states

import radical.entk.exceptions as ree

try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
class TestTask(TestCase):


    # --------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.generate_id', return_value='test.0000')
    def test_task_initialization(self, mocked_generate_id):
        '''
        **Purpose**: Test if the task attributes have, thus expect, the correct
        data types
        '''

        t = Task()

        self.assertEqual(t._uid, 'test.0000')
        self.assertEqual(t._name, '')
        self.assertIsNone(t._rts_uid)
        self.assertEqual(t._state, states.INITIAL)
        self.assertEqual(t._state_history, [states.INITIAL])
        self.assertEqual(t._executable, '')
        self.assertIsInstance(t._arguments, list)
        self.assertIsInstance(t._pre_exec, list)
        self.assertIsInstance(t._post_exec, list)

        self.assertEqual(t._cpu_reqs['processes'], 1)
        self.assertIsNone(t._cpu_reqs['process_type'])
        self.assertEqual(t._cpu_reqs['threads_per_process'], 1)
        self.assertIsNone(t._cpu_reqs['thread_type'])
        self.assertEqual(t._gpu_reqs['processes'], 0)
        self.assertIsNone(t._gpu_reqs['process_type'])
        self.assertEqual(t._gpu_reqs['threads_per_process'], 0)
        self.assertIsNone(t._gpu_reqs['thread_type'])

        self.assertEqual(t._lfs_per_process, 0)
        self.assertEqual(t._sandbox, '')
        self.assertIsInstance(t._upload_input_data, list)
        self.assertIsInstance(t._copy_input_data, list)
        self.assertIsInstance(t._link_input_data, list)
        self.assertIsInstance(t._move_input_data, list)
        self.assertIsInstance(t._copy_output_data, list)
        self.assertIsInstance(t._link_output_data, list)
        self.assertIsInstance(t._move_output_data, list)
        self.assertIsInstance(t._download_output_data, list)
        self.assertEqual(t._stdout, '')
        self.assertEqual(t._stderr, '')
        self.assertIsNone(t._exit_code)
        self.assertIsNone(t._tags)
        self.assertIsNone(t._path)
        self.assertIsNone(t._p_pipeline['uid'])
        self.assertIsNone(t._p_pipeline['name'])
        self.assertIsNone(t._p_stage['name'])
        self.assertIsNone(t._p_stage['uid'])


    # --------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.generate_id', return_value='test.0000')
    @mock.patch.object(Task, '__init__',   return_value=None)
    def test_cpu_reqs(self, mocked_generate_id, mocked_init):
        task = Task()
        task._cpu_reqs = {'processes'           : 1,
                          'process_type'        : None,
                          'threads_per_process' : 1,
                          'thread_type'         : None}
        cpu_reqs = {'processes' : 2,
                    'process_type' : None,
                    'threads_per_process' : 1,
                    'thread_type' : 'OpenMP'}
        task.cpu_reqs = {'processes' : 2,
                         'process_type' : None,
                         'threads_per_process' : 1,
                         'thread_type' : 'OpenMP'}

        self.assertEqual(task._cpu_reqs, cpu_reqs)
        self.assertEqual(task.cpu_reqs, {'cpu_processes' : 2,
                                         'cpu_process_type' : None,
                                         'cpu_threads' : 1,
                                         'cpu_thread_type' : 'OpenMP'})

        with self.assertRaises(ree.MissingError):
            task.cpu_reqs = {'cpu_processes' : 2,
                             'cpu_process_type' : None,
                             'cpu_thread_type' : 'OpenMP'}

        with self.assertRaises(ree.TypeError):
            task.cpu_reqs = {'cpu_processes' : 'a',
                             'cpu_process_type' : None,
                             'cpu_threads' : 1,
                             'cpu_thread_type' : 'OpenMP'}

        with self.assertRaises(ree.TypeError):
            task.cpu_reqs = {'cpu_processes' : 1,
                             'cpu_process_type' : None,
                             'cpu_threads' : 'a',
                             'cpu_thread_type' : 'OpenMP'}

        with self.assertRaises(ree.TypeError):
            task.cpu_reqs = list()

        with self.assertRaises(ree.ValueError):
            task.cpu_reqs = {'cpu_processes' : 1,
                             'cpu_process_type' : None,
                             'cpu_threads' : 1,
                             'cpu_thread_type' : 'MPI'}

        with self.assertRaises(ree.ValueError):
            task.cpu_reqs = {'cpu_processes' : 1,
                             'cpu_process_type' : 'test',
                             'cpu_threads' : 1,
                             'cpu_thread_type' : 'OpenMP'}


    # --------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.generate_id', return_value='test.0000')
    @mock.patch.object(Task, '__init__',   return_value=None)
    def test_gpu_reqs(self, mocked_generate_id, mocked_init):
        task = Task()
        task._gpu_reqs = {'processes'           : 1,
                          'process_type'        : None,
                          'threads_per_process' : 1,
                          'thread_type'         : None}
        gpu_reqs = {'processes' : 2,
                    'process_type' : None,
                    'threads_per_process' : 1,
                    'thread_type' : 'OpenMP'}
        task.gpu_reqs = {'processes' : 2,
                         'process_type' : None,
                         'threads_per_process' : 1,
                         'thread_type' : 'OpenMP'}

        self.assertEqual(task._gpu_reqs, gpu_reqs)
        self.assertEqual(task.gpu_reqs, {'gpu_processes' : 2,
                                         'gpu_process_type' : None,
                                         'gpu_threads' : 1,
                                         'gpu_thread_type' : 'OpenMP'})

        with self.assertRaises(ree.TypeError):
            task.gpu_reqs = list()

        with self.assertRaises(ree.MissingError):
            task.gpu_reqs = {'gpu_processes' : 2,
                             'gpu_process_type' : None,
                             'gpu_thread_type' : 'OpenMP'}

        with self.assertRaises(ree.TypeError):
            task.gpu_reqs = {'gpu_processes' : 'a',
                             'gpu_process_type' : None,
                             'gpu_threads' : 1,
                             'gpu_thread_type' : 'OpenMP'}

        with self.assertRaises(ree.TypeError):
            task.gpu_reqs = {'gpu_processes' : 1,
                             'gpu_process_type' : None,
                             'gpu_threads' : 'a',
                             'gpu_thread_type' : 'OpenMP'}

        with self.assertRaises(ree.ValueError):
            task.gpu_reqs = {'gpu_processes' : 1,
                             'gpu_process_type' : None,
                             'gpu_threads' : 1,
                             'gpu_thread_type' : 'MPI'}

        with self.assertRaises(ree.ValueError):
            task.gpu_reqs = {'gpu_processes' : 1,
                             'gpu_process_type' : 'test',
                             'gpu_threads' : 1,
                             'gpu_thread_type' : 'OpenMP'}

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Task, '__init__',   return_value=None)
    def test_uid(self, mocked_init):

        task = Task()
        task._uid = 'test.0000'
        self.assertEqual(task.uid, 'test.0000')

        task.uid = 'test.0001'
        self.assertEqual(task._uid, 'test.0001')

        with self.assertRaises(ree.TypeError):
            task.uid = 1

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Task, '__init__',   return_value=None)
    def test_luid(self, mocked_init):

        task = Task()
        task._name = ""
        task.parent_pipeline = {'name':'p0'}
        task.parent_stage = {'name':'s0'}
        task.name = 'test.0000'
        self.assertEqual(task.luid, 'p0.s0.test.0000')

        task = Task()
        task._name = ""
        task.parent_pipeline = {'uid':'p0'}
        task.parent_stage = {'uid':'s0'}
        task.uid = 'test.0000'
        self.assertEqual(task.luid, 'p0.s0.test.0000')

    # --------------------------------------------------------------------------
    #
    def test_dict_to_task(self):

        d = {'name'      : 'foo',
            'pre_exec'  : ['bar'],
            'executable': 'buz',
            'arguments' : ['baz', 'fiz'],
            'cpu_reqs'  : {'processes'          : 1,
                            'process_type'       : None,
                            'threads_per_process': 1,
                            'thread_type'        : None},
            'gpu_reqs'  : {'processes'          : 0,
                            'process_type'       : None,
                            'threads_per_process': 0,
                            'thread_type'        : None}}
        t = Task(from_dict=d)

        for k,v in d.items():
            self.assertEqual(t.__getattribute__(k), v, msg='%s != %s'
                % (t.__getattribute__(k), v))

        d = {'name'      : 'foo',
            'pre_exec'  : ['bar'],
            'executable': 'buz',
            'arguments' : ['baz', 'fiz'],
            'cpu_reqs'  : {'processes'          : 1,
                            'process_type'       : None,
                            'threads_per_process': 1,
                            'thread_type'        : None},
            'gpu_reqs'  : {'processes'          : 0,
                            'process_type'       : None,
                            'threads_per_process': 0,
                            'thread_type'        : None}}
        t = Task()
        t.from_dict(d)

        for k,v in d.items():
            self.assertEqual(t.__getattribute__(k), v, msg='%s != %s'
                % (t.__getattribute__(k), v))

        # make sure the type checks kick in
        d = 'test'
        with pytest.raises(ree.TypeError):
            t = Task(from_dict=d)


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Task, '__init__',   return_value=None)
    def test_name(self, mocked_init):

        task = Task()
        task._uid = 'test'
        task._name = 'test_name'
        self.assertEqual(task.name, 'test_name')

        task.name = 'task.0000'
        self.assertEqual(task._name, 'task.0000')

        with self.assertRaises(ree.TypeError):
            task.name = 0
        
        with self.assertRaises(ree.ValueError):
            task.name = 'task,0000'


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Task, '__init__',   return_value=None)
    def test_state_history(self, mocked_init):

        task = Task()
        task._uid = 'test'
        task._state_history = [states.INITIAL]
        self.assertEqual(task.state_history, [states.INITIAL])

        task.state_history = [states.SCHEDULED]
        self.assertEqual(task._state_history, [states.SCHEDULED])

        with self.assertRaises(ree.TypeError):
            task.state_history = states.SCHEDULING

        with self.assertRaises(ree.ValueError):
            task.state_history = ['EXECUTING']

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Task, '__init__',   return_value=None)
    def test_pre_exec(self, mocked_init):

        task = Task()
        task._pre_exec = ['module load mymodule']
        self.assertEqual(task.pre_exec, ['module load mymodule'])
        
        task.pre_exec = ['module load mymodule2']
        self.assertEqual(task._pre_exec, ['module load mymodule2'])
        with self.assertRaises(ree.TypeError):
            task.pre_exec = 'module load mymodule'


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Task, '__init__',   return_value=None)
    def test_arguments(self, mocked_init):

        task = Task()
        task._arguments = ['module load mymodule']
        self.assertEqual(task.arguments, ['module load mymodule'])
        
        task.arguments = ['module load mymodule2']
        self.assertEqual(task._arguments, ['module load mymodule2'])
        with self.assertRaises(ree.TypeError):
            task.arguments = 'module load mymodule'


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Task, '__init__',   return_value=None)
    def test_sandbox(self, mocked_init):

        task = Task()
        task._sandbox = '/path/to/a/sandbox'
        self.assertEqual(task.sandbox, '/path/to/a/sandbox')
        
        task.sandbox = '/path_to_a_sandbox'
        self.assertEqual(task._sandbox, '/path_to_a_sandbox')
        with self.assertRaises(ree.TypeError):
            task.sandbox = []
    
    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Task, '__init__',   return_value=None)
    def test_post_exec(self, mocked_init):

        task = Task()
        task._post_exec = ['module load mymodule']
        self.assertEqual(task.post_exec, ['module load mymodule'])
        
        task.post_exec = ['module load mymodule2']
        self.assertEqual(task._post_exec, ['module load mymodule2'])
        with self.assertRaises(ree.TypeError):
            task.post_exec = 'module load mymodule'


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Task, '__init__',   return_value=None)
    def test_tag(self, mocked_init):

        task = Task()
        task._tags = {'colocate':'tasks'}
        self.assertEqual(task.tag, {'colocate':'tasks'})
        
        task.tag = 'task'
        self.assertEqual(task._tags, {'colocate':'task'})
        with self.assertRaises(ree.TypeError):
            task.tag = {'colocate':'tasks'}


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Task, '__init__',   return_value=None)
    def test_tags(self, mocked_init):

        task = Task()
        task._tags = {'colocate':'tasks'}
        self.assertEqual(task.tag, {'colocate':'tasks'})
        
        task.tags = {'colocate':'task'}
        self.assertEqual(task._tags, {'colocate':'task'})

        with self.assertRaises(ree.TypeError):
            task.tags = 'task'

        with self.assertRaises(ree.TypeError):
            task.tags = {'key':'task'}
    def test_task_to_dict(self, mocked_init):

        t = Task()

        t._uid = 'test.0000'
        t._name = ''
        t._rts_uid = 'unit.0000'
        t._state = states.INITIAL
        t._state_history = [states.INITIAL]
        t._executable = ''
        t._arguments = list()
        t._pre_exec = list()
        t._post_exec = list()
        t._cpu_reqs = dict()
        t._cpu_reqs['processes'] = 1
        t._cpu_reqs['process_type'] = 'POSIX'
        t._cpu_reqs['threads_per_process'] = 1
        t._cpu_reqs['thread_type'] = 'POSIX'
        t._gpu_reqs = dict()
        t._gpu_reqs['processes'] = 0
        t._gpu_reqs['process_type'] = 'POSIX'
        t._gpu_reqs['threads_per_process'] = 0
        t._gpu_reqs['thread_type'] = 'POSIX'
        t._lfs_per_process = 0
        t._sandbox = ''
        t._upload_input_data = list()
        t._copy_input_data = list()
        t._link_input_data = list()
        t._move_input_data = list()
        t._copy_output_data = list()
        t._link_output_data = list()
        t._move_output_data = list()
        t._download_output_data = list()
        t._stdout = 'Hello World'
        t._stderr = 'Hello World'
        t._exit_code = 0
        t._tag = None
        t._path = 'some_path'
        t._p_pipeline = dict()
        t._p_pipeline['uid'] = 'pipe.0000'
        t._p_pipeline['name'] = 'pipe.0000'
        t._p_stage = dict()
        t._p_stage['name'] = 'stage.0000'
        t._p_stage['uid'] = 'stage.0000'

        expected_dict = {'uid': 'test.0000',
                         'name': '',
                         'state': 'DESCRIBED',
                         'state_history': ['DESCRIBED'],
                         'pre_exec': [],
                         'executable': '',
                         'arguments': [],
                         'sandbox': '',
                         'post_exec': [],
                         'cpu_reqs': {'cpu_processes': 1,
                                      'cpu_process_type': 'POSIX',
                                      'cpu_threads': 1,
                                      'cpu_thread_type': 'POSIX'},
                         'gpu_reqs': {'gpu_processes': 0,
                                      'gpu_process_type': 'POSIX',
                                      'gpu_threads': 0,
                                      'gpu_thread_type': 'POSIX'},
                         'lfs_per_process': 0,
                         'upload_input_data': [],
                         'copy_input_data': [],
                         'link_input_data': [],
                         'move_input_data': [],
                         'copy_output_data': [],
                         'link_output_data': [],
                         'move_output_data': [],
                         'download_output_data': [],
                         'stdout': 'Hello World',
                         'stderr': 'Hello World',
                         'exit_code': 0,
                         'path': 'some_path',
                         'tag': None,
                         'rts_uid': 'unit.0000',
                         'parent_stage': {'name': 'stage.0000',
                                          'uid': 'stage.0000'},
                         'parent_pipeline': {'uid': 'pipe.0000',
                                             'name': 'pipe.0000'}}

        task_dict = t.to_dict()

        self.assertEqual(task_dict, expected_dict)
