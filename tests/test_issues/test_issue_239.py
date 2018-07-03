from radical.entk import Task
import pytest

def test_issue_239():

    t = Task()
    t.cpu_reqs = {'processes': 1}
    assert t.cpu_reqs == {  'processes': 1, 
                            'thread_type': None, 
                            'threads_per_process': 1, 
                            'process_type': None}

    t.cpu_reqs = {'threads_per_process': 1}
    assert t.cpu_reqs == {  'processes': 1, 
                            'thread_type': None, 
                            'threads_per_process': 1, 
                            'process_type': None}

    t.gpu_reqs = {'processes': 1}
    assert t.gpu_reqs == {  'processes': 1, 
                            'thread_type': None, 
                            'threads_per_process': 1, 
                            'process_type': None}

    t.gpu_reqs = {'threads_per_process': 1}
    assert t.gpu_reqs == {  'processes': 1, 
                            'thread_type': None, 
                            'threads_per_process': 1, 
                            'process_type': None}