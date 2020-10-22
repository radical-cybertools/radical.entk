# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

__copyright__ = "Copyright 2020, http://radical.rutgers.edu"
__license__   = "MIT"

from unittest import TestCase

import os
import radical.utils as ru

from radical.entk.task import Task


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

        assert(task._cpu_reqs == cpu_reqs)
        assert(task.cpu_reqs == {'cpu_processes' : 2, 
                                 'cpu_process_type' : None, 
                                 'cpu_threads_per_process' : 1, 
                                 'cpu_thread_type' : 'OpenMP'})


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

        assert(task._gpu_reqs == gpu_reqs)
        assert(task.gpu_reqs == {'gpu_processes' : 2, 
                                 'gpu_process_type' : None, 
                                 'gpu_threads_per_process' : 1, 
                                 'gpu_thread_type' : 'OpenMP'})
