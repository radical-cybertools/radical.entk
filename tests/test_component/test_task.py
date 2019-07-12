
import os
import glob
import shutil
import pytest

from radical.entk            import Task
from radical.entk            import states
from radical.entk.exceptions import TypeError, ValueError, MissingError

from   hypothesis import given, settings
import hypothesis.strategies as st

# Hypothesis settings
settings.register_profile("travis", max_examples=100, deadline=None)
settings.load_profile("travis")


# ------------------------------------------------------------------------------
#
def test_task_initialization():

    """
    **Purpose**: Test if the task attributes have, thus expect, the correct data types
    """

    t = Task()
    assert t._uid                             is None
    assert t.name                             is None

    assert t.state                            == states.INITIAL
    assert t.state_history                    == [states.INITIAL]

    assert t.executable                       is None
    assert t.arguments                        == list()
    assert t.pre_exec                         == list()
    assert t.post_exec                        == list()

    assert t.cpu_reqs['processes']            == 1
    assert t.cpu_reqs['process_type']         is None
    assert t.cpu_reqs['threads_per_process']  == 1
    assert t.cpu_reqs['thread_type']          is None
    assert t.gpu_reqs['processes']            == 0
    assert t.gpu_reqs['process_type']         is None
    assert t.gpu_reqs['threads_per_process']  == 0
    assert t.gpu_reqs['thread_type']          is None
    assert t.lfs_per_process                  == 0

    assert t.upload_input_data                == list()
    assert t.copy_input_data                  == list()
    assert t.link_input_data                  == list()
    assert t.move_input_data                  == list()
    assert t.copy_output_data                 == list()
    assert t.move_input_data                  == list()
    assert t.download_output_data             == list()

    assert t.stdout                           is None
    assert t.stderr                           is None
    assert t.exit_code                        is None
    assert t.tag                              is None
    assert t.path                             is None

    assert t.parent_pipeline['uid']           is None
    assert t.parent_pipeline['name']          is None
    assert t.parent_stage['uid']              is None
    assert t.parent_stage['name']             is None


# ------------------------------------------------------------------------------
#
@given(s=st.text(),
       l=st.lists(st.text()),
       i=st.integers().filter(lambda x: type(x) == int),
       b=st.booleans())
def test_task_exceptions(s, l, i, b):
    """
    **Purpose**: Test if all attribute assignments raise exceptions
                 for invalid values
    """

    t = Task()

    data_type = [s, l, i, b]

    for data in data_type:

        if not isinstance(data,basestring):
            with pytest.raises(TypeError):
                print data, type(data)
                t.name = data

            with pytest.raises(TypeError):
                t.path = data

            with pytest.raises(TypeError):
                t.parent_stage = data

            with pytest.raises(TypeError):
                t.parent_pipeline = data

            with pytest.raises(TypeError):
                t.stdout = data

            with pytest.raises(TypeError):
                t.stderr = data

        if not isinstance(data,list):

            with pytest.raises(TypeError):
                t.pre_exec = data

            with pytest.raises(TypeError):
                t.arguments = data

            with pytest.raises(TypeError):
                t.post_exec = data

            with pytest.raises(TypeError):
                t.upload_input_data = data

            with pytest.raises(TypeError):
                t.copy_input_data = data

            with pytest.raises(TypeError):
                t.link_input_data = data

            with pytest.raises(TypeError):
                t.move_input_data = data

            with pytest.raises(TypeError):
                t.copy_output_data = data

            with pytest.raises(TypeError):
                t.download_output_data = data

            with pytest.raises(TypeError):
                t.move_output_data = data

        if not isinstance(data, basestring) and not isinstance(data, list):

            with pytest.raises(TypeError):
                t.executable = data

        if not isinstance(data, basestring) and not isinstance(data, unicode):

            with pytest.raises(ValueError):

                t.cpu_reqs = {'processes'           : 1,
                              'process_type'        : data,
                              'threads_per_process' : 1,
                              'thread_type'         : None}

                t.cpu_reqs = {'processes'           : 1,
                              'process_type'        : None,
                              'threads_per_process' : 1,
                              'thread_type'         : data}

                t.gpu_reqs = {'processes'           : 1,
                              'process_type'        : data,
                              'threads_per_process' : 1,
                              'thread_type'         : None}

                t.gpu_reqs = {'processes'           : 1,
                              'process_type'        : None,
                              'threads_per_process' : 1,
                              'thread_type'         : data}

        if not isinstance(data, int):

            with pytest.raises(TypeError):

                t.cpu_reqs = {'processes'           : data,
                              'process_type'        : None,
                              'threads_per_process' : 1,
                              'thread_type'         : None}

                t.cpu_reqs = {'processes'           : 1,
                              'process_type'        : None,
                              'threads_per_process' : data,
                              'thread_type'         : None}

                t.gpu_reqs = {'processes'           : data,
                              'process_type'        : None,
                              'threads_per_process' : 1,
                              'thread_type'         : None}

                t.gpu_reqs = {'processes'           : 1,
                              'process_type'        : None,
                              'threads_per_process' : data,
                              'thread_type'         : None}


# ------------------------------------------------------------------------------
#
def test_task_to_dict():
    """
    **Purpose**: Test if the 'to_dict' function of Task class converts all
                 expected attributes of the Task into a dictionary
    """

    t = Task()
    d = t.to_dict()

    assert d == {'uid'                : None,
            'name'                    : None,
            'state'                   : states.INITIAL,
            'state_history'           : [states.INITIAL],
            'pre_exec'                : [],
            'executable'              : None,
            'arguments'               : [],
            'post_exec'               : [],
            'cpu_reqs'                : {'processes'           : 1,
                                         'process_type'        : None,
                                         'threads_per_process' : 1,
                                         'thread_type'         : None},
            'gpu_reqs'                : {'processes'           : 0,
                                         'process_type'        : None,
                                         'threads_per_process' : 0,
                                         'thread_type'         : None},
            'lfs_per_process'         : 0,
            'upload_input_data'       : [],
            'copy_input_data'         : [],
            'link_input_data'         : [],
            'move_input_data'         : [],
            'copy_output_data'        : [],
            'move_output_data'        : [],
            'download_output_data'    : [],
            'stdout'                  : None,
            'stderr'                  : None,
            'exit_code'               : None,
            'path'                    : None,
            'tag'                     : None,
            'parent_stage'            : {'uid' : None, 'name' : None},
            'parent_pipeline'         : {'uid' : None, 'name' : None}}


    t = Task()
    t.uid                             = 'test.0017'
    t.name                            = 'new'
    t.pre_exec                        = ['module load abc']
    t.executable                      = 'sleep'
    t.arguments                       = ['10']
    t.cpu_reqs['processes']           = 10
    t.cpu_reqs['threads_per_process'] = 2
    t.gpu_reqs['processes']           = 5
    t.gpu_reqs['threads_per_process'] = 3
    t.lfs_per_process                 = 1024
    t.upload_input_data               = ['test1']
    t.copy_input_data                 = ['test2']
    t.link_input_data                 = ['test3']
    t.move_input_data                 = ['test4']
    t.copy_output_data                = ['test5']
    t.move_output_data                = ['test6']
    t.download_output_data            = ['test7']
    t.stdout                          = 'out'
    t.stderr                          = 'err'
    t.exit_code                       = 1
    t.path                            = 'a/b/c'
    t.tag                             = 'task.0010'
    t.parent_stage                    = {'uid': 's1', 'name': 'stage1'}
    t.parent_pipeline                 = {'uid': 'p1', 'name': 'pipeline1'}

    d = t.to_dict()

    assert d == {'uid'                  : 'test.0017',
                 'name'                 : 'new',
                 'state'                : states.INITIAL,
                 'state_history'        : [states.INITIAL],
                 'pre_exec'             : ['module load abc'],
                 'executable'           : 'sleep',
                 'arguments'            : ['10'],
                 'post_exec'            : [],
                 'cpu_reqs'             : {'processes'           : 10,
                                           'process_type'        : None,
                                           'threads_per_process' : 2,
                                           'thread_type'         : None},
                 'gpu_reqs'             : {'processes'           : 5,
                                           'process_type'        : None,
                                           'threads_per_process' : 3,
                                           'thread_type'         : None},
                 'lfs_per_process'      : 1024,
                 'upload_input_data'    : ['test1'],
                 'copy_input_data'      : ['test2'],
                 'link_input_data'      : ['test3'],
                 'move_input_data'      : ['test4'],
                 'copy_output_data'     : ['test5'],
                 'move_output_data'     : ['test6'],
                 'download_output_data' : ['test7'],
                 'stdout'               : 'out',
                 'stderr'               : 'err',
                 'exit_code'            : 1,
                 'path'                 : 'a/b/c',
                 'tag'                  : 'task.0010',
                 'parent_stage'         : {'uid': 's1', 'name' : 'stage1'},
                 'parent_pipeline'      : {'uid': 'p1', 'name' : 'pipeline1'}}


    t.executable = 'sleep'
    d = t.to_dict()

    assert d == {'uid'                  : 'test.0017',
                 'name'                 : 'new',
                 'state'                : states.INITIAL,
                 'state_history'        : [states.INITIAL],
                 'pre_exec'             : ['module load abc'],
                 'executable'           : 'sleep',
                 'arguments'            : ['10'],
                 'post_exec'            : [],
                 'cpu_reqs'             : {'processes'           : 10,
                                           'process_type'        : None,
                                           'threads_per_process' : 2,
                                           'thread_type'         : None},
                 'gpu_reqs'             : {'processes'           : 5,
                                           'process_type'        : None,
                                           'threads_per_process' : 3,
                                           'thread_type'         : None},
                 'lfs_per_process'      : 1024,
                 'upload_input_data'    : ['test1'],
                 'copy_input_data'      : ['test2'],
                 'link_input_data'      : ['test3'],
                 'move_input_data'      : ['test4'],
                 'copy_output_data'     : ['test5'],
                 'move_output_data'     : ['test6'],
                 'download_output_data' : ['test7'],
                 'stdout'               : 'out',
                 'stderr'               : 'err',
                 'exit_code'            : 1,
                 'path'                 : 'a/b/c',
                 'tag'                  : 'task.0010',
                 'parent_stage'         : {'uid': 's1', 'name' : 'stage1'},
                 'parent_pipeline'      : {'uid': 'p1', 'name' : 'pipeline1'}}


# ------------------------------------------------------------------------------
#
def test_task_from_dict():
    """
    **Purpose**: Test if the 'from_dict' function of Task class converts a
                 dictionary into a Task correctly with all the expected
                 attributes
    """

    d = {'uid'                  : 're.Task.0000',
         'name'                 : 't1',
         'state'                : states.DONE,
         'state_history'        : [states.INITIAL, states.DONE],
         'pre_exec'             : [],
         'executable'           : '',
         'arguments'            : [],
         'post_exec'            : [],
         'cpu_reqs'             : {'processes'           : 1,
                                   'process_type'        : None,
                                   'threads_per_process' : 1,
                                   'thread_type'         : None},
         'gpu_reqs'             : {'processes'           : 0,
                                   'process_type'        : None,
                                   'threads_per_process' : 0,
                                   'thread_type'         : None},
         'lfs_per_process'      : 1024,
         'upload_input_data'    : [],
         'copy_input_data'      : [],
         'link_input_data'      : [],
         'move_input_data'      : [],
         'copy_output_data'     : [],
         'move_output_data'     : [],
         'download_output_data' : [],
         'stdout'               : 'out',
         'stderr'               : 'err',
         'exit_code'            : 555,
         'path'                 : 'here/it/is',
         'tag'                  : 'task.0010',
         'parent_stage'         : {'uid': 's1', 'name' : 'stage1'},
         'parent_pipeline'      : {'uid': 'p1', 'name' : 'pipe1'}}

    t = Task()
    t.from_dict(d)

    assert t._uid                  == d['uid']
    assert t.name                  == d['name']
    assert t.state                 == d['state']
    assert t.state_history         == d['state_history']
    assert t.pre_exec              == d['pre_exec']
    assert t.executable            == d['executable']
    assert t.arguments             == d['arguments']
    assert t.post_exec             == d['post_exec']
    assert t.cpu_reqs              == d['cpu_reqs']
    assert t.gpu_reqs              == d['gpu_reqs']
    assert t.lfs_per_process       == d['lfs_per_process']
    assert t.upload_input_data     == d['upload_input_data']
    assert t.copy_input_data       == d['copy_input_data']
    assert t.link_input_data       == d['link_input_data']
    assert t.move_input_data       == d['move_input_data']
    assert t.copy_output_data      == d['copy_output_data']
    assert t.move_output_data      == d['move_output_data']
    assert t.download_output_data  == d['download_output_data']
    assert t.stdout                == d['stdout']
    assert t.stderr                == d['stderr']
    assert t.exit_code             == d['exit_code']
    assert t.path                  == d['path']
    assert t.tag                   == d['tag']
    assert t.parent_stage          == d['parent_stage']
    assert t.parent_pipeline       == d['parent_pipeline']


    d['executable'] = 'sleep'
    t = Task()
    t.from_dict(d)
    assert t.executable == d['executable']


# ------------------------------------------------------------------------------
#
def test_task_assign_uid():

    t = Task()
    try:
        home   = os.environ.get('HOME', '/home')
        folder = glob.glob('%s/.radical/utils/test*' % home)

        for f in folder:
            shutil.rmtree(f)
    except:
        pass

    t._assign_uid('test')
    assert t.uid == 'task.0000'


# ------------------------------------------------------------------------------
#
def test_task_validate():

    t = Task()
    t._state = 'test'
    with pytest.raises(ValueError):
        t._validate()

    t = Task()
    with pytest.raises(MissingError):
        t._validate()


# ------------------------------------------------------------------------------

