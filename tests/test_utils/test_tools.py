# pylint: disable=protected-access

import os
import shutil
import tempfile

from unittest import TestCase, mock

import radical.utils              as ru

import radical.entk               as re
import radical.entk.tools.darshan as re_darshan


# ------------------------------------------------------------------------------
#
class TestTools(TestCase):

    # --------------------------------------------------------------------------
    #
    def test_cache_darshan_env(self):

        re_darshan._darshan_activation_cmds = None
        re_darshan._darshan_env             = None
        re_darshan._darshan_runtime_root    = None

        with self.assertRaises(RuntimeError):
            # should be set with either env variable or an absolute path
            re_darshan.cache_darshan_env(darshan_runtime_root='dir_name')

        re_darshan.cache_darshan_env(darshan_runtime_root='$DARSHAN_ROOT_TEST',
                                     modules=['test_module'],
                                     env={'TEST_VAR': 'test_value'})

        self.assertEqual(re_darshan._darshan_runtime_root, '$DARSHAN_ROOT_TEST')
        self.assertEqual(re_darshan._darshan_activation_cmds,
                         ['module load test_module',
                          'export TEST_VAR="test_value"'])

        self.assertTrue('TEST_VAR' in re_darshan._darshan_env)

    # --------------------------------------------------------------------------
    #
    def test_enable_darshan(self):

        with self.assertRaises(ValueError):
            # empty or no object is provided
            re_darshan.enable_darshan(pst=None)

        with self.assertRaises(TypeError):
            # in case of list - only list of Pipelines is allowed
            re_darshan.enable_darshan(pst=[re.Task()])

        with self.assertRaises(TypeError):
            # only PST objects are allowed
            re_darshan.enable_darshan(pst='random_string')

        task = re.Task()
        task.cpu_reqs = {'cpu_processes': 1}

        task.executable = ''
        re_darshan.enable_darshan(task)
        # Darshan is not enabled for tasks with no executable
        self.assertFalse(task.executable)

        # enable darshan using decorator
        @re_darshan.with_darshan
        def get_pipeline():
            t1 = re.Task()
            t1.cpu_reqs   = {'cpu_processes': 1}
            t1.executable = 'test_exec1'
            t2 = re.Task()
            t2.cpu_reqs = {'cpu_processes': 10}
            t2.executable = 'test_exec2'
            s0 = re.Stage()
            s0.add_tasks([t1, t2])
            p0 = re.Pipeline()
            p0.add_stages(s0)
            return p0

        pipeline = get_pipeline()
        for s in pipeline.stages:
            for t in s.tasks:
                self.assertTrue('LD_PRELOAD' in t.executable)

                non_mpi_statement = 'DARSHAN_ENABLE_NONMPI=1' in t.executable
                if t.cpu_reqs.cpu_processes == 1:
                    # non-MPI task
                    self.assertTrue(non_mpi_statement)
                else:
                    self.assertFalse(non_mpi_statement)

                # darshan is already enabled
                re_darshan.enable_darshan(t)
                self.assertEqual(t.executable.count('LD_PRELOAD'), 1)

    # --------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.sh_callout',
                return_value=['1:/tmp/test_file.txt\n0:random\n', '', 0])
    def test_annotate_task_with_darshan(self, mocked_sh_callout):

        tmp_dir = tempfile.gettempdir()
        log_dir = os.path.join(tmp_dir, 'darshan_logs')
        ru.rec_makedir(log_dir)

        # keeping inside log_dir a random file
        tempfile.mkstemp(dir=log_dir)

        task = re_darshan.enable_darshan(re.Task({
            'executable': 'test_exec',
            'arguments' : ['> arg_output.txt'],
            'cpu_reqs'  : {'cpu_processes': 1},
            'sandbox'   : tmp_dir,
            'path'      : tmp_dir
        }))

        re_darshan.annotate_task_with_darshan(task)

        ta = task.annotations
        # file "test_file.txt" is presented in both "inputs" and "outputs",
        # because the way we mocked method "sh_callout"
        self.assertIn('/test_file.txt',  ' '.join(ta['inputs']))
        self.assertIn('/test_file.txt',  ' '.join(ta['outputs']))
        self.assertIn('/arg_output.txt', ' '.join(ta['outputs']))

        self.assertNotIn('random', ' '.join(ta['outputs']))

        shutil.rmtree(log_dir)

# ------------------------------------------------------------------------------

