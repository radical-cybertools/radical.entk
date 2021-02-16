# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

from unittest import TestCase

from radical.entk.execman.rp.task_processor import create_td_from_task
from radical.entk                           import Task


class TestBase(TestCase):

    # ------------------------------------------------------------------------------
    #
    def test_create_td_from_task(self):

        pipeline_name = 'pipe.0000'
        stage_name    = 'stage.0000'
        t1_name       = 'task.0000'
        t2_name       = 'task.0001'

        placeholders = {
            pipeline_name: {
                stage_name: {
                    t1_name: {
                        'path'   : '/home/vivek/t1',
                        'uid': 'task.0000'
                    },
                    t2_name: {
                        'path'   : '/home/vivek/t2',
                        'uid': 'task.0003'
                    }
                }
            }
        }

        task = Task()
        task.uid = 'task.0000' 
        task.name = 'task.0000'
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

        test_td = create_td_from_task(task, placeholders)
        self.assertEqual(test_td.name, 'task.0000,task.0000,stage.0000,stage.0000,pipe.0000,pipe.0000')
        self.assertEqual(test_td.pre_exec, ['post_exec'])
        self.assertEqual(test_td.executable, '/bin/date')
        self.assertEqual(test_td.arguments, ['test_args'])
        self.assertEqual(test_td.sandbox, 'unit.0000')
        self.assertEqual(test_td.post_exec, [''])
        self.assertEqual(test_td.cpu_processes, 5)
        self.assertEqual(test_td.cpu_threads, 6)
        self.assertEqual(test_td.cpu_process_type, 'MPI')
        self.assertIsNone(test_td.cpu_thread_type)
        self.assertEqual(test_td.gpu_processes, 5)
        self.assertEqual(test_td.gpu_threads, 6)
        self.assertEqual(test_td.gpu_process_type, None)
        self.assertIsNone(test_td.gpu_thread_type)
        self.assertEqual(test_td.lfs_per_process, 235)
        self.assertEqual(test_td.stdout, 'stdout')
        self.assertEqual(test_td.stderr, 'stderr')
        self.assertEqual(test_td.input_staging, [])
        self.assertEqual(test_td.output_staging, [])
        self.assertEqual(test_td.tag, 'task.0000')

        task.tags = {'colocate': 'task.0001'}
        test_td = create_td_from_task(task, placeholders)
        self.assertEqual(test_td.tag, 'task.0003')

