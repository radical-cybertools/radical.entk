# pylint: disable=protected-access

from unittest import TestCase

import radical.entk               as re
import radical.entk.tools.darshan as re_darshan


# ------------------------------------------------------------------------------
#
class TestTools(TestCase):

    # --------------------------------------------------------------------------
    #
    def test_cache_darshan_env(self):

        with self.assertRaises(RuntimeError):
            # should be set with either env variable or an absolute path
            re_darshan.cache_darshan_env(darshan_runtime_root='dir_name')

        self.assertIsNone(re_darshan._darshan_activation_cmds)
        self.assertIsNone(re_darshan._darshan_env)
        self.assertIsNone(re_darshan._darshan_runtime_root)

        re_darshan.cache_darshan_env(darshan_runtime_root='$DARSHAN_ROOT',
                                     modules=['test_module'],
                                     env={'TEST_VAR': 'test_value'})

        self.assertEqual(re_darshan._darshan_runtime_root, '$DARSHAN_ROOT')
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

        task.executable = 'test_exec'
        re_darshan.enable_darshan(task)
        # `test_cache_darshan_env` had already activated the env
        self.assertTrue('LD_PRELOAD' in task.executable)
        # non-MPI task
        self.assertTrue('DARSHAN_ENABLE_NONMPI=1' in task.executable)

# ------------------------------------------------------------------------------

