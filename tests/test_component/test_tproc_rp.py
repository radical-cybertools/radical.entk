# pylint: disable=protected-access,unused-argument,no-value-for-parameter

import radical.pilot as rp

from unittest import mock, TestCase

import radical.entk            as re
import radical.entk.exceptions as ree

from radical.entk.task import Annotations

from radical.entk.execman.rp.task_processor import resolve_placeholders
from radical.entk.execman.rp.task_processor import resolve_arguments
from radical.entk.execman.rp.task_processor import resolve_tags
from radical.entk.execman.rp.task_processor import get_input_list_from_task
from radical.entk.execman.rp.task_processor import get_output_list_from_task
from radical.entk.execman.rp.task_processor import create_td_from_task
from radical.entk.execman.rp.task_processor import create_task_from_rp


# ------------------------------------------------------------------------------
#
class TestBase(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.Logger')
    def test_resolve_placeholders(self, mocked_logger):

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

        p = ['test_file > $SHARED/test_file',
             'test_file > $Pipeline_%s_Stage_%s_Task_%s/test_file' % (
                 pipeline_name, stage_name, t1_name),
             # unknown variable cannot be resolved
             'test_file > $NODE_LFS_PATH/test.txt']

        self.assertEqual(resolve_placeholders(p[0], placeholders, mock.Mock()),
                         'test_file > pilot:///test_file')
        self.assertEqual(resolve_placeholders(p[1], placeholders, mock.Mock()),
                         'test_file > /home/vivek/t1/test_file')
        with self.assertRaises(ree.EnTKValueError):
            resolve_placeholders(p[2], placeholders, mock.Mock())

    # --------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.Logger')
    def test_resolve_arguments(self, mocked_logger):

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

        a = ['$SHARED',
             '$Pipeline_%s_Stage_%s_Task_%s' % (
                 pipeline_name, stage_name, t1_name),
             '$Pipeline_%s_Stage_%s_Task_%s' % (
                 pipeline_name, stage_name, t2_name),
             '$NODE_LFS_PATH/test.txt']

        self.assertEqual(resolve_arguments(a, placeholders, mock.Mock()),
                         ['$RP_PILOT_SANDBOX',
                          '/home/vivek/t1',
                          '/home/vivek/t2',
                          '$NODE_LFS_PATH/test.txt'])

    # --------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.Logger')
    def test_resolve_tags(self, mocked_logger):

        pipeline_name = 'p1'
        stage_name    = 's1'

        task = mock.Mock()
        task.uid  = 'task.0000'
        task.tags = {'colocate': task.uid, 'exclusive': False}

        task2 = mock.Mock()
        task2.uid  = 'task.0001'
        task2.tags = None
        t2_name    = 't2'

        placeholders = {
            pipeline_name: {
                stage_name: {
                    task.uid: {
                        'path': '/home/vivek/t1',
                        'uid' : 'unit.0002'
                    },
                    t2_name: {
                        'path': '/home/vivek/t2',
                        'uid' : 'unit.0003'
                    }
                }
            }
        }

        self.assertEqual(resolve_tags(task=task,
                                      parent_pipeline_name=pipeline_name,
                                      placeholders=placeholders),
                         {'colocate': 'unit.0002', 'exclusive': False})

        self.assertEqual(resolve_tags(task=task2,
                                      parent_pipeline_name=pipeline_name,
                                      placeholders=placeholders),
                         {'colocate': 'task.0001'})

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(re.execman.rp.task_processor,
                       'resolve_arguments',
                       return_value=['test_args_resolved'])
    @mock.patch.object(re.execman.rp.task_processor,
                       'resolve_tags',
                       return_value={'colocate': 'test_tags_resolved'})
    @mock.patch.object(re.execman.rp.task_processor,
                       'get_input_list_from_task',
                       return_value=['test_inputs_resolved'])
    @mock.patch.object(re.execman.rp.task_processor,
                       'get_output_list_from_task',
                       return_value=['test_outputs_resolved'])
    @mock.patch('radical.utils.generate_id', return_value='task.0000.0000')
    @mock.patch('radical.utils.Logger')
    def test_create_td_from_task(self, mocked_logger, mocked_generate_id,
                                       mocked_get_output_list_from_task,
                                       mocked_get_input_list_from_task,
                                       mocked_resolve_tags,
                                       mocked_resolve_arguments):

        task = mock.Mock()
        task.uid             = 'task.0000'
        task.name            = 'task.name'
        task.parent_stage    = {'uid' : 'stage.0000',
                                'name': 'stage.0000'}
        task.parent_pipeline = {'uid' : 'pipe.0000',
                                'name': 'pipe.0000'}
        task.pre_launch      = ['pre_launch']
        task.pre_exec        = ['pre_exec']
        task.executable      = '/bin/date'
        task.arguments       = ['test_args']
        task.sandbox         = 'unit.0000'
        task.post_exec       = ['']
        task.post_launch     = []
        task.environment     = {}
        task.cpu_reqs        = {'cpu_processes'   : 5,
                                'cpu_threads'     : 6,
                                'cpu_thread_type' : ''}
        task.gpu_reqs        = {'gpu_processes'   : 1.,
                                'gpu_process_type': ''}
        task.tags            = None
        task.lfs_per_process = 235
        task.mem_per_process = 128
        task.stderr          = 'stderr'
        task.stdout          = 'stdout'
        task.metadata        = {}

        task.annotations     = Annotations({'input'     : ['task.0001:f1.txt'],
                                            'output'    : [],
                                            'depends_on': ['task.0001']})

        hash_table = {}
        test_td = create_td_from_task(task=task,
                                      placeholders=None,
                                      task_hash_table=hash_table,
                                      pkl_path='.test.pkl',
                                      sid='test.sid',
                                      logger=mocked_logger)

        self.assertEqual(test_td.name, 'task.0000,task.name,stage.0000,'
                                       'stage.0000,pipe.0000,pipe.0000')
        self.assertEqual(test_td.pre_launch, ['pre_launch'])
        self.assertEqual(test_td.pre_exec, ['pre_exec'])
        self.assertEqual(test_td.executable, '/bin/date')
        self.assertEqual(test_td.arguments, ['test_args_resolved'])
        self.assertEqual(test_td.sandbox, 'unit.0000')
        self.assertEqual(test_td.post_exec, [''])
        self.assertEqual(test_td.post_launch, [])
        self.assertEqual(test_td.environment, {})
        self.assertEqual(test_td.ranks,          5)
        self.assertEqual(test_td.cores_per_rank, 6)
        self.assertEqual(test_td.threading_type, rp.POSIX)
        self.assertEqual(test_td.gpus_per_rank,  1.)
        self.assertEqual(test_td.gpu_type,       '')
        self.assertEqual(test_td.lfs_per_process, 235)
        self.assertEqual(test_td.mem_per_process, 128)
        self.assertEqual(test_td.stdout, 'stdout')
        self.assertEqual(test_td.stderr, 'stderr')
        self.assertEqual(test_td.input_staging, ['test_inputs_resolved'])
        self.assertEqual(test_td.output_staging, ['test_outputs_resolved'])
        self.assertEqual(test_td.tags, {'colocate': 'test_tags_resolved'})
        self.assertEqual(test_td.uid, 'task.0000')
        self.assertEqual(hash_table, {'task.0000': 'task.0000'})

        self.assertEqual(test_td.metadata['data']['output'], [])
        self.assertEqual(test_td.metadata['data']['depends_on'], ['task.0001'])

        task.cpu_reqs = {'cpu_processes'   : 1,
                         'cpu_threads'     : 2,
                         'cpu_thread_type' : None}
        task.gpu_reqs = {'gpu_processes'   : 3.,
                         'gpu_process_type': None}

        test_td = create_td_from_task(task=task,
                                      placeholders=None,
                                      task_hash_table=hash_table,
                                      pkl_path='.test.pkl',
                                      sid='test.sid',
                                      logger=mocked_logger)

        self.assertEqual(test_td.ranks,          1)
        self.assertEqual(test_td.cores_per_rank, 2)
        self.assertEqual(test_td.threading_type, rp.POSIX)
        self.assertEqual(test_td.gpus_per_rank,  3.)
        self.assertEqual(test_td.gpu_type,       '')

    # --------------------------------------------------------------------------
    #
    @mock.patch('radical.entk.Task')
    @mock.patch('radical.utils.Logger')
    def test_create_task_from_rp(self, mocked_logger, mocked_task):

        test_rp_task = mock.Mock()
        test_rp_task.name                = 'task.0000,task.0000,stage.0000,' \
                                           'stage.0000,pipe.0000,pipe.0000'
        test_rp_task.uid                 = 'unit.0000'
        test_rp_task.state               = 'EXECUTING'
        test_rp_task.sandbox             = 'test_folder'
        test_rp_task.description         = {}

        mocked_task.uid             = None
        mocked_task.name            = None
        mocked_task.parent_stage    = {}
        mocked_task.parent_pipeline = {}
        mocked_task.path            = None
        mocked_task.rts_uid         = None

        task = create_task_from_rp(test_rp_task, mock.Mock())

        self.assertEqual(task.uid, 'task.0000')
        self.assertEqual(task.name, 'task.0000')
        self.assertEqual(task.parent_stage,    {'uid' : 'stage.0000',
                                                'name': 'stage.0000'})
        self.assertEqual(task.parent_pipeline, {'uid' : 'pipe.0000',
                                                'name': 'pipe.0000'})
        self.assertEqual(task.path, 'test_folder')
        self.assertEqual(task.rts_uid, 'unit.0000')

    # --------------------------------------------------------------------------
    #
    @mock.patch('radical.entk.Task')
    @mock.patch('radical.utils.Logger')
    def test_issue_271(self, mocked_logger, mocked_task):

        test_rp_task = mock.Mock()
        test_rp_task.name                = 'task.0000,task.0000,stage.0000,' \
                                           'stage.0000,pipe.0000,pipe.0000'
        test_rp_task.uid                 = 'unit.0000'
        test_rp_task.state               = 'DONE'
        test_rp_task.sandbox             = 'test_folder'
        test_rp_task.description         = {}

        mocked_task.uid             = None
        mocked_task.name            = None
        mocked_task.parent_stage    = {}
        mocked_task.parent_pipeline = {}
        mocked_task.path            = None
        mocked_task.rts_uid         = None
        mocked_task.metadata        = {}

        task = create_task_from_rp(test_rp_task, mock.Mock())
        self.assertIsNone(task.exit_code)

    # --------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.Logger')
    def test_get_input_list_from_task(self, mocked_logger):

        task = mock.Mock()

        with self.assertRaises(ree.EnTKTypeError):
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

        task = mock.MagicMock(spec=re.Task)
        task.link_input_data = [
            '$SHARED/test_folder/test_file > test_folder/test_file']
        task.upload_input_data = [
            '$SHARED/test_folder/test_file > test_file']
        task.copy_input_data = [
            '$Pipeline_p1_Stage_s1_Task_t1/test_file > $SHARED/test_file']
        task.move_input_data = [
            'test_file > test_file']

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

    # --------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.Logger')
    def test_get_output_list_from_task(self, mocked_logger):

        task = mock.Mock()

        with self.assertRaises(ree.EnTKTypeError):
            get_output_list_from_task(task, '', mock.Mock())

        task = mock.MagicMock(spec=re.Task)
        task.link_output_data     = ['test_file > $SHARED/test_file']
        task.download_output_data = ['test_file > $SHARED/test_file']
        task.copy_output_data     = ['test_file > $SHARED/test_file']
        task.move_output_data     = ['test_file > $SHARED/test_file']
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

