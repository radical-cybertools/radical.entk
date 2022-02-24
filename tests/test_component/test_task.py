# pylint: disable=protected-access, unused-argument, no-member

__copyright__ = 'Copyright 2020-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import radical.entk.exceptions as ree
import radical.entk.states     as res

from radical.entk.task import Task

from unittest import mock, TestCase


# ------------------------------------------------------------------------------
#
class TestTask(TestCase):

    # --------------------------------------------------------------------------
    #
    def test_init(self):

        t = Task()

        self.assertTrue(t.uid.startswith('task.'))
        self.assertEqual(t.name, '')
        self.assertIsNone(t.rts_uid)
        self.assertEqual(t.state, res.INITIAL)
        self.assertEqual(t.state_history, [res.INITIAL])
        self.assertEqual(t.executable, '')
        self.assertIsInstance(t.arguments, list)
        self.assertIsInstance(t.pre_exec, list)
        self.assertIsInstance(t.post_exec, list)

        # instance's attribute(s) access(es)
        self.assertEqual(t.cpu_reqs.cpu_processes, 1)
        # instance's attribute and item accesses
        self.assertEqual(t.cpu_reqs['cpu_processes'], 1)
        self.assertIsNone(t.cpu_reqs['cpu_process_type'])
        self.assertEqual(t.cpu_reqs['cpu_threads'], 1)
        self.assertIsNone(t.cpu_reqs['cpu_thread_type'])
        self.assertEqual(t.gpu_reqs['gpu_processes'], 0)
        self.assertIsNone(t.gpu_reqs['gpu_process_type'])
        self.assertEqual(t.gpu_reqs['gpu_threads'], 1)
        self.assertIsNone(t.gpu_reqs['gpu_thread_type'])

        self.assertEqual(t.lfs_per_process, 0)
        self.assertEqual(t.mem_per_process, 0)
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

        input_data = {'state'        : res.SCHEDULED,
                      'state_history': [res.INITIAL,
                                        res.SCHEDULING,
                                        res.SCHEDULED]}
        task = Task(from_dict=input_data)
        self.assertEqual(task.state, input_data['state'])
        self.assertEqual(task.state_history, input_data['state_history'])

        with self.assertRaises(ree.TypeError):
            # incorrect type of input data
            Task(from_dict='input_str')

    # --------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.generate_id', return_value='initial.uid.0000')
    def test_uid(self, mocked_generate_id):

        task = Task()

        self.assertEqual(task.uid, 'initial.uid.0000')

        task = Task(from_dict={'uid': 'test.0000'})
        self.assertEqual(task.uid, 'test.0000')

        with self.assertRaises(ree.EnTKError):
            Task(from_dict={'uid': 'test:0000'})

    # --------------------------------------------------------------------------
    #
    def test_cpu_reqs(self):

        cpu_reqs = {'cpu_processes'   : 2,
                    'cpu_process_type': None,
                    'cpu_threads'     : 1,
                    'cpu_thread_type' : 'OpenMP'}

        task = Task(from_dict={'cpu_reqs': cpu_reqs})
        self.assertEqual(Task.demunch(task.cpu_reqs), cpu_reqs)

        # obsolete names for `cpu_reqs` keys
        cpu_reqs_obsolete_keys = {'processes'          : 2,
                                  'process_type'       : None,
                                  'threads_per_process': 1,
                                  'thread_type'        : 'OpenMP'}

        task = Task(from_dict={'cpu_reqs': cpu_reqs_obsolete_keys})
        self.assertEqual(Task.demunch(task.cpu_reqs), cpu_reqs)

        threads_per_process = 64
        task.cpu_reqs.threads_per_process = threads_per_process
        self.assertEqual(task.cpu_reqs.cpu_threads, threads_per_process)

        with self.assertRaises(TypeError):
            # incorrect type for `cpu_reqs` attribute
            task.cpu_reqs = list()

        with self.assertRaises(TypeError):
            # incorrect type of `cpu_reqs.cpu_processes` attribute
            task.cpu_reqs = {'cpu_processes'   : 'a',
                             'cpu_process_type': None,
                             'cpu_threads'     : 1,
                             'cpu_thread_type' : 'OpenMP'}

        with self.assertRaises(TypeError):
            # incorrect type of `cpu_reqs.cpu_threads` attribute
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

        # obsolete names for `gpu_reqs` keys
        gpu_reqs_obsolete_keys = {'processes'          : 2,
                                  'process_type'       : None,
                                  'threads_per_process': 1,
                                  'thread_type'        : 'OpenMP'}

        task = Task(from_dict={'gpu_reqs': gpu_reqs_obsolete_keys})
        self.assertEqual(Task.demunch(task.gpu_reqs), gpu_reqs)

        threads_per_process = 64
        task.gpu_reqs.threads_per_process = threads_per_process
        self.assertEqual(task.gpu_reqs.gpu_threads, threads_per_process)

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
    def test_luid(self):

        task = Task()
        task.name            = 'test.0000'
        task.parent_pipeline = {'name': 'p0'}
        task.parent_stage    = {'name': 's0'}
        self.assertEqual(task.luid, 'p0.s0.test.0000')

        task = Task({'uid': 'test.0001'})
        task.parent_pipeline = {'uid': 'p1'}
        task.parent_stage    = {'uid': 's1'}
        self.assertEqual(task.luid, 'p1.s1.test.0001')

    # --------------------------------------------------------------------------
    #
    def test_from_dict(self):

        task = Task()

        # check that default attributes are set
        task_uid_saved = task.uid
        self.assertTrue(task_uid_saved)
        self.assertEqual(task.name, '')

        task.from_dict({'name': 'test.name'})
        # task was re-initialized with new "uid", which was auto-generated
        self.assertNotEqual(task.uid, task_uid_saved)
        self.assertEqual(task.name, 'test.name')

        task.from_dict({'uid': 'user.uid.00'})
        # task was re-initialized with provided "uid"
        self.assertEqual(task.uid, 'user.uid.00')

    # --------------------------------------------------------------------------
    #
    def test_from_dict_extended(self):

        input_dict = {
            'name'      : 'foo',
            'pre_exec'  : ['bar'],
            'executable': 'buz',
            'arguments' : ['baz', 'fiz'],
            'cpu_reqs'  : {'cpu_processes'   : 1,
                           'cpu_process_type': None,
                           'cpu_threads'     : 1,
                           'cpu_thread_type' : None},
            'gpu_reqs'  : {'gpu_processes'   : 0,
                           'gpu_process_type': None,
                           'gpu_threads'     : 0,
                           'gpu_thread_type' : None}
        }
        task = Task(from_dict=input_dict)
        for k, v in input_dict.items():
            self.assertEqual(Task.demunch(task[k]), v)

        input_dict = {
            'name'      : 'foo',
            'pre_exec'  : ['bar'],
            'executable': 'buz',
            'arguments' : ['baz', 'fiz'],
            'state'     : res.SUBMITTING,
            'cpu_reqs'  : {'cpu_processes'   : 1,
                           'cpu_process_type': None,
                           'cpu_threads'     : 1,
                           'cpu_thread_type' : None}}
        task = Task()
        task.from_dict(input_dict)

        for k, v in input_dict.items():
            self.assertEqual(Task.demunch(task[k]), v)

    # --------------------------------------------------------------------------
    #
    def test_verify(self):

        task = Task()
        task.executable = 'test_exec'
        self.assertEqual(task.executable, 'test_exec')

        task = Task()
        Task._cast = False  # if True, then `executable` will be "['test_exec']"
        with self.assertRaises(TypeError):
            task.executable = ['test_exec']

        task = Task()
        with self.assertRaises(ree.EnTKError):
            # not allowed to re-assign "uid" attribute
            task.uid = 'new.uid'

        with self.assertRaises(ree.EnTKError):
            # not correct name format (only [a-zA-Z\.] are allowed in name)
            task.name = 'new_name'

        with self.assertRaises(ree.ValueError):
            # unknown state
            task.state = 'UNKNOWN_STATE'

        with self.assertRaises(ree.ValueError):
            # state history with unknown state
            task.state_history = ['UNKNOWN_STATE']

        with self.assertRaises(ree.EnTKError):
            # tags of a wrong format, only one key "colocate" is allowed
            task.tags = {'colocate': 'tag01', 'unknown_key': 'tag02'}

        # since all attributes are checked during setting process method
        # `verify` is not needed, but could be called to ensure data correctness
        task.verify()

    # --------------------------------------------------------------------------
    #
    def test_validate(self):

        task1 = Task({'executable': 'exec.sh'})
        task1._validate()

        task2 = Task({'uid': task1.uid})
        with self.assertRaises(ree.EnTKError):
            # task with the same "uid" was already created
            task2._validate()

        self.assertIn(task1.uid, Task._uids)
        self.assertEqual(len(Task._uids), 1)

        task3 = Task({'state': res.SCHEDULED})
        with self.assertRaises(ree.ValueError):
            # validation should be called when the task is in the initial state
            task3._validate()

        task4 = Task()
        with self.assertRaises(ree.MissingError):
            # attribute "executable" is not set
            task4._validate()

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
            'mem_per_process'     : 0,
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
        self.assertEqual(task.as_dict(), expected_dict)

    # --------------------------------------------------------------------------
    #
    def test_state_history(self):

        task = Task()

        self.assertEqual(task.state_history, [res.INITIAL])

        task.state = res.SCHEDULED
        self.assertEqual(task.state_history, [res.INITIAL, res.SCHEDULED])

        task.state_history = [res.DONE]
        self.assertEqual(task.state_history, [res.DONE])

        with self.assertRaises(ree.ValueError):
            task.state_history = ['WRONG_STATE_NAME']

    # --------------------------------------------------------------------------
    #
    def test_eq_hash(self):

        t1 = Task()
        t2 = Task()

        self.assertNotEqual(t1, t2)
        self.assertNotEqual(hash(t1), hash(t2))

        self.assertNotEqual(t1, t1.as_dict())
        self.assertNotEqual(t2, t2.as_dict())

        tasks_set = {t1, t2}
        self.assertIsInstance(tasks_set, set)
        self.assertEqual(len(tasks_set), 2)

        uid = 'non.unique.uid'
        t1.from_dict({'uid': uid})
        t2.from_dict({'uid': uid})
        self.assertEqual(t1, t2)
        self.assertEqual(hash(t1), hash(t2))

        t1['name'] = 'unique.name.1'
        t2['name'] = 'unique.name.2'
        tasks_set = {t1, t2}
        self.assertIsInstance(tasks_set, set)
        self.assertEqual(len(tasks_set), 1)
        # 2nd task wasn't added since it has "uid" as the task in the set
        self.assertEqual(list(tasks_set)[0]['name'], 'unique.name.1')


# ------------------------------------------------------------------------------

