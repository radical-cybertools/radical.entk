# pylint: disable=protected-access,unused-argument,no-value-for-parameter

import pickle
import radical.pilot as rp

from unittest import mock, TestCase

from radical.entk.execman.rp.task_processor import create_td_from_task
from radical.entk                           import Task


# ------------------------------------------------------------------------------
#
class TestBase(TestCase):

    # --------------------------------------------------------------------------
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
                        'path': '/home/vivek/t1',
                        'uid' : 'task.0000'
                    },
                    t2_name: {
                        'path': '/home/vivek/t2',
                        'uid' : 'task.0003'
                    }
                }
            }
        }

        task = Task({'uid': 'task.0000'})
        task.name            = 'task.0000'
        task.parent_stage    = {'uid' : 'stage.0000',
                                'name': 'stage.0000'}
        task.parent_pipeline = {'uid' : 'pipe.0000',
                                'name': 'pipe.0000'}
        task.pre_launch      = ['pre_launch_cmd']
        task.pre_exec        = ['pre_exec_cmd']
        task.executable      = '/bin/date'
        task.arguments       = ['test_args']
        task.sandbox         = 'task.0000'
        task.post_exec       = ['']
        task.post_launch     = []
        task.environment     = {}
        task.cpu_reqs        = {'cpu_processes'   : 5,
                                'cpu_threads'     : 6,
                                'cpu_thread_type' : rp.OpenMP}
        task.gpu_reqs        = {'gpu_processes'   : 1,
                                'gpu_process_type': rp.CUDA}

        task.lfs_per_process = 235
        task.stderr          = 'stderr'
        task.stdout          = 'stdout'

        hash_table = {}
        test_td = create_td_from_task(task, placeholders, hash_table,
                                      '.test.pkl', 'test_sid', mock.Mock())
        self.assertIsInstance(test_td, rp.TaskDescription)
        self.assertEqual(test_td.name, 'task.0000,task.0000,stage.0000,stage.0000,pipe.0000,pipe.0000')
        self.assertEqual(test_td.pre_launch, ['pre_launch_cmd'])
        self.assertEqual(test_td.pre_exec, ['pre_exec_cmd'])
        self.assertEqual(test_td.executable, '/bin/date')
        self.assertEqual(test_td.arguments, ['test_args'])
        self.assertEqual(test_td.sandbox, 'task.0000')
        self.assertEqual(test_td.post_exec, [''])
        self.assertEqual(test_td.post_launch, [])
        self.assertEqual(test_td.environment, {})
        self.assertEqual(test_td.ranks,          5)
        self.assertEqual(test_td.cores_per_rank, 6)
        self.assertEqual(test_td.threading_type, rp.OpenMP)
        self.assertEqual(test_td.gpus_per_rank,  1)
        self.assertEqual(test_td.gpu_type,       rp.CUDA)
        self.assertEqual(test_td.lfs_per_process, 235)
        self.assertEqual(test_td.stdout, 'stdout')
        self.assertEqual(test_td.stderr, 'stderr')
        self.assertEqual(test_td.input_staging, [])
        self.assertEqual(test_td.output_staging, [])
        self.assertEqual(test_td.tags, {'colocate': 'task.0000'})
        self.assertEqual(test_td.uid, 'task.0000')
        self.assertEqual(hash_table, {'task.0000': 'task.0000'})
        task.tags = {'colocate': 'task.0001'}
        test_td = create_td_from_task(task, placeholders, hash_table,
                                      '.test.pkl', 'test_sid', mock.Mock())
        self.assertEqual(test_td.tags, {'colocate': 'task.0003'})
        self.assertEqual(test_td.uid, 'task.0000.0000')
        self.assertEqual(hash_table, {'task.0000': 'task.0000.0000'})
        with open('.test.pkl', 'rb') as f:
            submitted_tasks = pickle.load(f)
        self.assertEqual(submitted_tasks, hash_table)

# ------------------------------------------------------------------------------

