# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

from unittest import TestCase

from radical.entk.execman.rp.task_processor import create_cud_from_task
from radical.entk                           import Task


class TestBase(TestCase):

    # ------------------------------------------------------------------------------
    #
    def test_create_cud_from_task(self):

        task = Task()
        task.uid = 'task.0000' 
        task.name = 'task.name'
        task.parent_stage = {'uid' : 'stage.0000',
                             'name' : 'stage.0000'}
        task.parent_pipeline = {'uid' : 'pipe.0000',
                                'name' : 'pipe.0000'}
        task.pre_exec = ['post_exec']
        task.executable = '/bin/date'
        task.arguments = ['test_args']
        task.sandbox = 'unit.0000'
        task.post_exec = ['']
        task.cpu_reqs = {'cpu_processes': 5,
                         'cpu_threads': 6,
                         'cpu_process_type': 'MPI',
                         'cpu_thread_type': None}
        task.gpu_reqs = {'gpu_processes': 5,
                         'gpu_threads': 6,
                         'gpu_process_type': None,
                         'gpu_thread_type': None}

        task.lfs_per_process = 235
        task.stderr = 'stderr'
        task.stdout = 'stdout'

        test_cud = create_cud_from_task(task, None)
        self.assertEqual(test_cud.name, 'task.0000,task.name,stage.0000,stage.0000,pipe.0000,pipe.0000')
        self.assertEqual(test_cud.pre_exec, ['post_exec'])
        self.assertEqual(test_cud.executable, '/bin/date')
        self.assertEqual(test_cud.arguments, ['test_args'])
        self.assertEqual(test_cud.sandbox, 'unit.0000')
        self.assertEqual(test_cud.post_exec, [''])
        self.assertEqual(test_cud.cpu_processes, 5)
        self.assertEqual(test_cud.cpu_threads, 6)
        self.assertEqual(test_cud.cpu_process_type, 'MPI')
        self.assertIsNone(test_cud.cpu_thread_type)
        self.assertEqual(test_cud.gpu_processes, 5)
        self.assertEqual(test_cud.gpu_threads, 6)
        self.assertEqual(test_cud.gpu_process_type, None)
        self.assertIsNone(test_cud.gpu_thread_type)
        self.assertEqual(test_cud.lfs_per_process, 235)
        self.assertEqual(test_cud.stdout, 'stdout')
        self.assertEqual(test_cud.stderr, 'stderr')
        self.assertEqual(test_cud.input_staging, [])
        self.assertEqual(test_cud.output_staging, [])
        self.assertEqual(test_cud.tag, 'task.name')

        task.tag = 'task.tag'
        test_cud = create_cud_from_task(task, None)
        self.assertEqual(test_cud.tag, 'task.tag')

