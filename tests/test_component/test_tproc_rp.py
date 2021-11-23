# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

from unittest import TestCase

import radical.entk.exceptions as ree

import radical.entk

from radical.entk.execman.rp.task_processor import resolve_placeholders
from radical.entk.execman.rp.task_processor import resolve_arguments
from radical.entk.execman.rp.task_processor import resolve_tags
from radical.entk.execman.rp.task_processor import get_input_list_from_task
from radical.entk.execman.rp.task_processor import get_output_list_from_task
from radical.entk.execman.rp.task_processor import create_td_from_task
from radical.entk.execman.rp.task_processor import create_task_from_rp

try:
    import mock
except ImportError:
    from unittest import mock



class TestBase(TestCase):

    # ------------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.Logger')
    def test_resolve_placeholders(self, mocked_Logger):

        pipeline_name = 'p1'
        stage_name    = 's1'
        t1_name       = 't1'

        placeholders = {
            pipeline_name: {
                stage_name: {
                    t1_name: {
                        'path'   : '/home/vivek/t1',
                        'rts_uid': 'unit.0002'
                    }
                }
            }
        }

        paths = ['test_file > $SHARED/test_file',
                 'test_file > $Pipeline_%s_Stage_%s_Task_%s/test_file' % (pipeline_name, stage_name, t1_name),
                 'test_file > $NODE_LFS_PATH/test.txt']

        self.assertEqual(resolve_placeholders(paths[0], placeholders,
            mock.Mock()), 'test_file > pilot:///test_file')
        self.assertEqual(resolve_placeholders(paths[1], placeholders,
            mock.Mock()), 'test_file > /home/vivek/t1/test_file')
        with self.assertRaises(ree.ValueError):
            resolve_placeholders(paths[2], placeholders, mock.Mock())


    # ------------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.Logger')
    def test_resolve_arguments(self, mocked_Logger):

        pipeline_name = 'p1'
        stage_name    = 's1'
        t1_name       = 't1'
        t2_name       = 't2'

        placeholders = {
            pipeline_name: {
                stage_name: {
                    t1_name: {
                        'path'   : '/home/vivek/t1',
                        'rts_uid': 'unit.0002'
                    },
                    t2_name: {
                        'path'   : '/home/vivek/t2',
                        'rts_uid': 'unit.0003'
                    }
                }
            }
        }

        arguments = ['$SHARED',
                    '$Pipeline_%s_Stage_%s_Task_%s' % (pipeline_name, stage_name, t1_name),
                    '$Pipeline_%s_Stage_%s_Task_%s' % (pipeline_name, stage_name, t2_name),
                    '$NODE_LFS_PATH/test.txt']

        self.assertEqual(resolve_arguments(arguments, placeholders, mock.Mock()),
                    ['$RP_PILOT_STAGING', '/home/vivek/t1',
                    '/home/vivek/t2',    '$NODE_LFS_PATH/test.txt'])


    # ------------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.Logger')
    def test_resolve_tags(self, mocked_Logger):

        pipeline_name = 'p1'
        stage_name    = 's1'
        task = mock.Mock()
        task.uid = 'task.0000'
        task.tags = {'colocate': task.uid}
        task2 = mock.Mock()
        task2.uid = 'task.0001'
        task2.tags = None
        t2_name       = 't2'

        placeholders = {
            pipeline_name: {
                stage_name: {
                    task.uid: {
                        'path'   : '/home/vivek/t1',
                        'uid': 'unit.0002'
                    },
                    t2_name: {
                        'path'   : '/home/vivek/t2',
                        'uid': 'unit.0003'
                    }
                }
            }
        }

        self.assertEqual(resolve_tags(task=task,
                            parent_pipeline_name=pipeline_name,
                            placeholders=placeholders),
                         {'colocate': 'unit.0002'})

        self.assertEqual(resolve_tags(task=task2, parent_pipeline_name=pipeline_name,
                         placeholders=placeholders), {'colocate':'task.0001'})

    # ------------------------------------------------------------------------------
    #
    @mock.patch('radical.pilot.TaskDescription')
    @mock.patch('radical.utils.Logger')
    @mock.patch.object(radical.entk.execman.rp.task_processor,
            'get_output_list_from_task', return_value='outputs')
    @mock.patch.object(radical.entk.execman.rp.task_processor,
            'resolve_arguments', return_value='test_args')
    @mock.patch.object(radical.entk.execman.rp.task_processor,
            'resolve_tags', return_value='test_tag')
    @mock.patch.object(radical.entk.execman.rp.task_processor,
            'get_input_list_from_task', return_value='inputs')
    @mock.patch('radical.utils.generate_id', return_value='task.0000.0000')
    def test_create_td_from_task(self, mocked_TaskDescription,
                                  mocked_Logger, mocked_get_input_list_from_task,
                                  mocked_get_output_list_from_task,
                                 mocked_resolve_arguments, mocked_resolve_tags,
                                 mocked_generate_id):

        mocked_TaskDescription.name             = None
        mocked_TaskDescription.pre_exec         = None
        mocked_TaskDescription.executable       = None
        mocked_TaskDescription.arguments        = None
        mocked_TaskDescription.sandbox          = None
        mocked_TaskDescription.post_exec        = None
        mocked_TaskDescription.tag              = None
        mocked_TaskDescription.cpu_processes    = None
        mocked_TaskDescription.cpu_threads      = None
        mocked_TaskDescription.cpu_process_type = None
        mocked_TaskDescription.cpu_thread_type  = None
        mocked_TaskDescription.gpu_processes    = None
        mocked_TaskDescription.gpu_threads      = None
        mocked_TaskDescription.gpu_process_type = None
        mocked_TaskDescription.gpu_thread_type  = None
        mocked_TaskDescription.lfs_per_process  = None
        mocked_TaskDescription.mem_per_process  = None
        mocked_TaskDescription.stdout           = None
        mocked_TaskDescription.stderr           = None
        mocked_TaskDescription.input_staging    = None
        mocked_TaskDescription.output_staging   = None

        task = mock.Mock()
        task.uid = 'task.0000'
        task.name = 'task.name'
        task.parent_stage = {'uid' : 'stage.0000',
                             'name' : 'stage.0000'}
        task.parent_pipeline = {'uid' : 'pipe.0000',
                                'name' : 'pipe.0000'}
        task.pre_exec = 'post_exec'
        task.executable = '/bin/date'
        task.arguments = 'test_arg'
        task.sandbox = 'unit.0000'
        task.post_exec = ''
        task.cpu_reqs = {'cpu_processes': 5,
                         'cpu_threads': 6,
                         'cpu_process_type': 'MPI',
                         'cpu_thread_type': 'MPI'}
        task.gpu_reqs = {'gpu_processes': 5,
                         'gpu_threads': 6,
                         'gpu_process_type': 'MPI',
                         'gpu_thread_type': 'MPI'}
        task.tags = None

        task.lfs_per_process = 235
        task.mem_per_process = 128
        task.stderr = 'stderr'
        task.stdout = 'stdout'
        hash_table = {}
        test_td = create_td_from_task(task=task, placeholders=None,
                                      task_hash_table=hash_table,
                                      pkl_path='.test.pkl',
                                      sid='test.sid', logger=mock.Mock())
        self.assertEqual(test_td.name, 'task.0000,task.name,stage.0000,stage.0000,pipe.0000,pipe.0000')
        self.assertEqual(test_td.pre_exec, 'post_exec')
        self.assertEqual(test_td.executable, '/bin/date')
        self.assertEqual(test_td.arguments, 'test_args')
        self.assertEqual(test_td.sandbox, 'unit.0000')
        self.assertEqual(test_td.post_exec, '')
        self.assertEqual(test_td.cpu_processes, 5)
        self.assertEqual(test_td.cpu_threads, 6)
        self.assertEqual(test_td.cpu_process_type, 'MPI')
        self.assertEqual(test_td.cpu_thread_type, 'MPI')
        self.assertEqual(test_td.gpu_processes, 5)
        self.assertEqual(test_td.gpu_threads, 6)
        self.assertEqual(test_td.gpu_process_type, 'MPI')
        self.assertEqual(test_td.gpu_thread_type, 'MPI')
        self.assertEqual(test_td.lfs_per_process, 235)
        self.assertEqual(test_td.mem_per_process, 128)
        self.assertEqual(test_td.stdout, 'stdout')
        self.assertEqual(test_td.stderr, 'stderr')
        self.assertEqual(test_td.input_staging, 'inputs')
        self.assertEqual(test_td.output_staging, 'outputs')
        self.assertEqual(test_td.uid, 'task.0000')
        self.assertEqual(hash_table, {'task.0000':'task.0000'})

        task.cpu_reqs = {'cpu_processes': 5,
                         'cpu_threads': 6,
                         'cpu_process_type': None,
                         'cpu_thread_type': None}
        task.gpu_reqs = {'gpu_processes': 5,
                         'gpu_threads': 6,
                         'gpu_process_type': None,
                         'gpu_thread_type': None}

        test_td = create_td_from_task(task=task, placeholders=None,
                                      task_hash_table=hash_table,
                                      pkl_path='.test.pkl',
                                      sid='test.sid', logger=mock.Mock())
        self.assertEqual(test_td.name, 'task.0000,task.name,stage.0000,stage.0000,pipe.0000,pipe.0000')
        self.assertEqual(test_td.pre_exec, 'post_exec')
        self.assertEqual(test_td.executable, '/bin/date')
        self.assertEqual(test_td.arguments, 'test_args')
        self.assertEqual(test_td.sandbox, 'unit.0000')
        self.assertEqual(test_td.post_exec, '')
        self.assertEqual(test_td.cpu_processes, 5)
        self.assertEqual(test_td.cpu_threads, 6)
        self.assertEqual(test_td.cpu_process_type, 'POSIX')
        self.assertEqual(test_td.cpu_thread_type, 'OpenMP')
        self.assertEqual(test_td.gpu_processes, 5)
        self.assertEqual(test_td.gpu_threads, 6)
        self.assertEqual(test_td.gpu_process_type, 'POSIX')
        self.assertEqual(test_td.gpu_thread_type, 'GPU_OpenMP')
        self.assertEqual(test_td.lfs_per_process, 235)
        self.assertEqual(test_td.mem_per_process, 128)
        self.assertEqual(test_td.stdout, 'stdout')
        self.assertEqual(test_td.stderr, 'stderr')
        self.assertEqual(test_td.input_staging, 'inputs')
        self.assertEqual(test_td.output_staging, 'outputs')
        self.assertEqual(test_td.uid, 'task.0000.0000')
        self.assertEqual(hash_table, {'task.0000':'task.0000.0000'})

    # ------------------------------------------------------------------------------
    #
    @mock.patch('radical.entk.Task')
    @mock.patch('radical.utils.Logger')
    def test_create_task_from_rp(self, mocked_Task, mocked_Logger):
        test_cud = mock.Mock()
        test_cud.name             = 'task.0000,task.0000,stage.0000,stage.0000,pipe.0000,pipe.0000'
        test_cud.pre_exec         = 'post_exec'
        test_cud.executable       = '/bin/date'
        test_cud.arguments        = 'test_args'
        test_cud.sandbox          = 'unit.0000'
        test_cud.post_exec        = ''
        test_cud.cpu_processes    = 5
        test_cud.cpu_threads      = 6
        test_cud.cpu_process_type = 'POSIX'
        test_cud.cpu_thread_type  = None
        test_cud.gpu_processes    = 5
        test_cud.gpu_threads      = 6
        test_cud.gpu_process_type = 'POSIX'
        test_cud.gpu_thread_type  = None
        test_cud.lfs_per_process  = 235
        test_cud.mem_per_process  = 128
        test_cud.stdout           = 'stdout'
        test_cud.stderr           = 'stderr'
        test_cud.input_staging    = 'inputs'
        test_cud.output_staging   = 'outputs'
        test_cud.uid              = 'unit.0000'
        test_cud.state            = 'EXECUTING'
        test_cud.sandbox          = 'test_folder'

        mocked_Task.uid             = None
        mocked_Task.name            = None
        mocked_Task.parent_stage    = {}
        mocked_Task.parent_pipeline = {}
        mocked_Task.path            = None
        mocked_Task.rts_uid         = None

        task = create_task_from_rp(test_cud, mock.Mock())
        self.assertEqual(task.uid, 'task.0000')
        self.assertEqual(task.name, 'task.0000')
        self.assertEqual(task.parent_stage, {'uid': 'stage.0000', 'name': 'stage.0000'})
        self.assertEqual(task.parent_pipeline, {'uid': 'pipe.0000', 'name': 'pipe.0000'})
        self.assertEqual(task.path, 'test_folder')
        self.assertEqual(task.rts_uid, 'unit.0000')


    # ------------------------------------------------------------------------------
    #
    @mock.patch('radical.entk.Task')
    @mock.patch('radical.utils.Logger')
    def test_issue_271(self, mocked_Task, mocked_Logger):
        test_cud = mock.Mock()
        test_cud.name             = 'task.0000,task.0000,stage.0000,stage.0000,pipe.0000,pipe.0000'
        test_cud.pre_exec         = 'post_exec'
        test_cud.executable       = '/bin/date'
        test_cud.arguments        = 'test_args'
        test_cud.sandbox          = 'unit.0000'
        test_cud.post_exec        = ''
        test_cud.cpu_processes    = 5
        test_cud.cpu_threads      = 6
        test_cud.cpu_process_type = 'POSIX'
        test_cud.cpu_thread_type  = None
        test_cud.gpu_processes    = 5
        test_cud.gpu_threads      = 6
        test_cud.gpu_process_type = 'POSIX'
        test_cud.gpu_thread_type  = None
        test_cud.lfs_per_process  = 235
        test_cud.mem_per_process  = 128
        test_cud.stdout           = 'stdout'
        test_cud.stderr           = 'stderr'
        test_cud.input_staging    = 'inputs'
        test_cud.output_staging   = 'outputs'
        test_cud.uid              = 'unit.0000'
        test_cud.state            = 'DONE'
        test_cud.sandbox          = 'test_folder'

        mocked_Task.uid             = None
        mocked_Task.name            = None
        mocked_Task.parent_stage    = {}
        mocked_Task.parent_pipeline = {}
        mocked_Task.path            = None
        mocked_Task.rts_uid         = None

        task = create_task_from_rp(test_cud, mock.Mock())
        self.assertEqual(task.exit_code, 0)

        test_cud.state = 'FAILED'
        task = create_task_from_rp(test_cud, mock.Mock())
        self.assertEqual(task.exit_code, 1)

        test_cud.state = 'EXECUTING'
        task = create_task_from_rp(test_cud, mock.Mock())
        self.assertIsNone(task.exit_code)

    # ------------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.Logger')
    def test_get_input_list_from_task(self, mocked_Logger):


        task = mock.Mock()

        with self.assertRaises(ree.TypeError):
            get_input_list_from_task(task, '', mock.Mock())

        pipeline_name = 'p1'
        stage_name    = 's1'
        t1_name       = 't1'

        placeholders = {
            pipeline_name: {
                stage_name: {
                    t1_name: {
                        'path'   : '/home/vivek/t1',
                        'rts_uid': 'unit.0002'
                    }
                }
            }
        }

        task = mock.MagicMock(spec=radical.entk.Task)
        task.link_input_data = ['$SHARED/test_folder/test_file > test_folder/test_file']
        task.upload_input_data = ['$SHARED/test_folder/test_file > test_file']
        task.copy_input_data = ['$Pipeline_p1_Stage_s1_Task_t1/test_file > $SHARED/test_file']
        task.move_input_data = ['test_file > test_file']
        test = get_input_list_from_task(task, placeholders, mock.Mock())

        input_list = [{'source': 'pilot:///test_folder/test_file',
                       'target': 'test_folder/test_file',
                       'action': 'Link'},
                      {'source': 'pilot:///test_folder/test_file',
                       'target': 'test_file'},
                      {'source': '/home/vivek/t1/test_file',
                       'target': 'pilot:///test_file',
                       'action': 'Copy'},
                      {'source': 'test_file',
                       'target': 'test_file',
                       'action': 'Move'}]

        self.assertEqual(test[0], input_list[0])
        self.assertEqual(test[1], input_list[1])
        self.assertEqual(test[2], input_list[2])
        self.assertEqual(test[3], input_list[3])

    # ------------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.Logger')
    def test_get_output_list_from_task(self, mocked_Logger):


        task = mock.Mock()

        with self.assertRaises(ree.TypeError):
            get_output_list_from_task(task, '', mock.Mock())

        task = mock.MagicMock(spec=radical.entk.Task)
        task.link_output_data = ['test_file > $SHARED/test_file']
        task.download_output_data = ['test_file > $SHARED/test_file']
        task.copy_output_data = ['test_file > $SHARED/test_file']
        task.move_output_data = ['test_file > $SHARED/test_file']
        test = get_output_list_from_task(task, {}, mock.Mock())

        output_list = [{'source': 'test_file',
                       'target': 'pilot:///test_file',
                       'action': 'Link'},
                      {'source': 'test_file',
                       'target': 'pilot:///test_file'},
                      {'source': 'test_file',
                       'target': 'pilot:///test_file',
                       'action': 'Copy'},
                      {'source': 'test_file',
                       'target': 'pilot:///test_file',
                       'action': 'Move'}]

        self.assertEqual(test[0], output_list[0])
        self.assertEqual(test[1], output_list[1])
        self.assertEqual(test[2], output_list[2])
        self.assertEqual(test[3], output_list[3])


    # ------------------------------------------------------------------------------
    #
    @mock.patch('radical.pilot.TaskDescription')
    @mock.patch('radical.utils.Logger')
    def test_issue_259(self, mocked_TaskDescription, mocked_Logger):

        mocked_TaskDescription.name             = None
        mocked_TaskDescription.pre_exec         = None
        mocked_TaskDescription.executable       = None
        mocked_TaskDescription.arguments        = None
        mocked_TaskDescription.sandbox          = None
        mocked_TaskDescription.post_exec        = None
        mocked_TaskDescription.tag              = None
        mocked_TaskDescription.cpu_processes    = None
        mocked_TaskDescription.cpu_threads      = None
        mocked_TaskDescription.cpu_process_type = None
        mocked_TaskDescription.cpu_thread_type  = None
        mocked_TaskDescription.gpu_processes    = None
        mocked_TaskDescription.gpu_threads      = None
        mocked_TaskDescription.gpu_process_type = None
        mocked_TaskDescription.gpu_thread_type  = None
        mocked_TaskDescription.lfs_per_process  = None
        mocked_TaskDescription.mem_per_process  = None
        mocked_TaskDescription.stdout           = None
        mocked_TaskDescription.stderr           = None
        mocked_TaskDescription.input_staging    = None
        mocked_TaskDescription.output_staging   = None

        pipeline_name = 'pipe.0000'
        stage_name    = 'stage.0000'
        t1_name       = 'task.0000'
        t2_name       = 'task.0001'

        placeholders = {
            pipeline_name: {
                stage_name: {
                    t1_name: {
                        'path'   : '/home/vivek/t1',
                        'uid': 'unit.0002'
                    },
                    t2_name: {
                        'path'   : '/home/vivek/t2',
                        'uid': 'unit.0003'
                    }
                }
            }
        }

        task = mock.MagicMock(spec=radical.entk.Task)
        task.uid = 'task.0000'
        task.name = 'task.0000'
        task.parent_stage = {'uid' : 'stage.0000',
                             'name' : 'stage.0000'}
        task.parent_pipeline = {'uid' : 'pipe.0000',
                                'name' : 'pipe.0000'}
        task.pre_exec = 'post_exec'
        task.executable = '/bin/date'
        task.arguments = ['$SHARED',
                          '$Pipeline_%s_Stage_%s_Task_%s' % (pipeline_name,
                                                             stage_name,
                                                             t1_name),
                          '$Pipeline_%s_Stage_%s_Task_%s' % (pipeline_name,
                                                             stage_name,
                                                             t2_name),
                          '$NODE_LFS_PATH/test.txt']
        task.sandbox = 'unit.0000'
        task.post_exec = ''
        task.cpu_reqs = {'cpu_processes': 5,
                         'cpu_threads': 6,
                         'cpu_process_type': 'POSIX',
                         'cpu_thread_type': None}
        task.gpu_reqs = {'gpu_processes': 5,
                         'gpu_threads': 6,
                         'gpu_process_type': 'POSIX',
                         'gpu_thread_type': None}
        task.tags = None

        task.lfs_per_process = 235
        task.mem_per_process = 128
        task.stderr = 'stderr'
        task.stdout = 'stdout'
        input_list = [{'source': 'test_file',
                       'target': 'pilot:///test_file',
                       'action': 'Link'},
                      {'source': 'test_file',
                       'target': 'pilot:///test_file'},
                      {'source': 'test_file',
                       'target': 'pilot:///test_file',
                       'action': 'Copy'},
                      {'source': 'test_file',
                       'target': 'pilot:///test_file',
                       'action': 'Move'}]
        task.link_input_data = ['test_file > $SHARED/test_file']
        task.upload_input_data = ['test_file > $SHARED/test_file']
        task.copy_input_data = ['test_file > $SHARED/test_file']
        task.move_input_data = ['test_file > $SHARED/test_file']
        output_list = [{'source': 'test_file',
                       'target': 'pilot:///test_file',
                       'action': 'Link'},
                      {'source': 'test_file',
                       'target': 'pilot:///test_file'},
                      {'source': 'test_file',
                       'target': 'pilot:///test_file',
                       'action': 'Copy'},
                      {'source': 'test_file',
                       'target': 'pilot:///test_file',
                       'action': 'Move'}]
        task.link_output_data = ['test_file > $SHARED/test_file']
        task.download_output_data = ['test_file > $SHARED/test_file']
        task.copy_output_data = ['test_file > $SHARED/test_file']
        task.move_output_data = ['test_file > $SHARED/test_file']
        hash_table = {}
        test_cud = create_td_from_task(task, placeholders, hash_table,
                                      pkl_path='.test.pkl',
                                      sid='test.sid', logger=mock.Mock())
        self.assertEqual(test_cud.name, 'task.0000,task.0000,stage.0000,stage.0000,pipe.0000,pipe.0000')
        self.assertEqual(test_cud.pre_exec, 'post_exec')
        self.assertEqual(test_cud.executable, '/bin/date')
        self.assertEqual(test_cud.arguments, ['$RP_PILOT_STAGING',
                                              '/home/vivek/t1',
                                              '/home/vivek/t2',
                                              '$NODE_LFS_PATH/test.txt'])
        self.assertEqual(test_cud.sandbox, 'unit.0000')
        self.assertEqual(test_cud.post_exec, '')
        self.assertEqual(test_cud.cpu_processes, 5)
        self.assertEqual(test_cud.cpu_threads, 6)
        self.assertEqual(test_cud.cpu_process_type, 'POSIX')
        self.assertEqual(test_cud.cpu_thread_type, 'OpenMP')
        self.assertEqual(test_cud.gpu_processes, 5)
        self.assertEqual(test_cud.gpu_threads, 6)
        self.assertEqual(test_cud.gpu_process_type, 'POSIX')
        self.assertEqual(test_cud.gpu_thread_type, 'GPU_OpenMP')
        self.assertEqual(test_cud.lfs_per_process, 235)
        self.assertEqual(test_cud.mem_per_process, 128)
        self.assertEqual(test_cud.stdout, 'stdout')
        self.assertEqual(test_cud.stderr, 'stderr')
        self.assertEqual(test_cud.input_staging, input_list)
        self.assertEqual(test_cud.output_staging, output_list)
