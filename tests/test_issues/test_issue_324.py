from radical.entk import Task, states

'''
Test if task.from_dict accepts executable as a str or list
'''

def test_issue_271():

    d = {   'uid': 're.Task.0000',
            'name': 't1',
            'state': states.DONE,
            'state_history': [states.INITIAL, states.DONE],
            'pre_exec': [],
            'executable': 'sleep',
            'arguments': [],
            'post_exec': [],
            'cpu_reqs': { 'processes': 1,
                        'process_type': None,
                        'threads_per_process': 1,
                        'thread_type': None
                        },
            'gpu_reqs': { 'processes': 0,
                        'process_type': None,
                        'threads_per_process': 0,
                        'thread_type': None
                        },
            'lfs_per_process': 1024,
            'upload_input_data': [],
            'copy_input_data': [],
            'link_input_data': [],
            'move_input_data': [],
            'copy_output_data': [],
            'move_output_data': [],
            'download_output_data': [],
            'stdout': 'out',
            'stderr': 'err',
            'exit_code': 555,
            'path': 'here/it/is',
            'tag': 'task.0010',
            'parent_stage': {'uid': 's1', 'name': 'stage1'},
            'parent_pipeline': {'uid': 'p1', 'name': 'pipe1'}}

    t = Task()
    t.from_dict(d)

    d['executable'] = 'sleep'

    t = Task()
    t.from_dict(d)
