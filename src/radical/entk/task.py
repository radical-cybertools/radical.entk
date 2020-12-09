
__copyright__ = 'Copyright 2014-2020, http://radical.rutgers.edu'
__license__   = 'MIT'

import radical.utils as ru

from . import exceptions as ree
from . import states     as res


# ------------------------------------------------------------------------------
#
class Task(ru.Munch):
    '''
    A Task is an abstraction of a computational unit. In this case, a Task
    consists of its executable along with its required software environment,
    files to be staged as input and output.

    At the user level a Task is to be populated by assigning attributes.
    Internally, an empty Task is created and population using the `from_dict`
    function. This is to avoid creating Tasks with new `uid` as tasks with new
    `uid` offset the uid count file in radical.utils and can potentially affect
    the profiling if not taken care.
    '''

    _schema = {'uid'                  : str,
               'name'                 : str,
               'state'                : str,
               'state_history'        : [str],
               'pre_exec'             : [str],
               'executable'           : str,
               'arguments'            : [str],
               'sandbox'              : str,
               'post_exec'            : [str],
               'cpu_reqs'             : {str: int, str: str, str: int, str: str},
               'gpu_reqs'             : {str: int, str: str, str: int, str: str},
               'lfs_per_process'      : int,
               'upload_input_data'    : [str],
               'copy_input_data'      : [str],
               'link_input_data'      : [str],
               'move_input_data'      : [str],
               'copy_output_data'     : [str],
               'link_output_data'     : [str],
               'move_output_data'     : [str],
               'download_output_data' : [str],
               'stdout'               : str,
               'stderr'               : str,
               'exit_code'            : int,
               'path'                 : str,
               'tag'                  : None,
               'rts_uid'              : str,
               'parent_stage'         : {str: None, str: None},
               'parent_pipeline'      : {str: None, str: None}
            }

    _defaults = {'uid'                  : ru.generate_id('task.%(counter)04d', ru.ID_CUSTOM),
                 'name'                 : '',
                 'state'                : res.INITIAL,
                 'state_history'        : [res.INITIAL],
                 'pre_exec'             : list(),
                 'executable'           : '',
                 'arguments'            : list(),
                 'sandbox'              : '',
                 'post_exec'            : list(),
                 'cpu_reqs'             : {'cpu_processes'           : 1,
                                           'cpu_process_type'        : None,
                                           'cpu_threads_per_process' : 1,
                                           'cpu_thread_type'         : None},
                 'lfs_per_process'      : 0,
                 'upload_input_data'    : list(),
                 'copy_input_data'      : list(),
                 'link_input_data'      : list(),
                 'move_input_data'      : list(),
                 'copy_output_data'     : list(),
                 'link_output_data'     : list(),
                 'move_output_data'     : list(),
                 'download_output_data' : list(),
                 'stdout'               : '',
                 'stderr'               : '',
                 'path'                 : '',
                 'tag'                  : None,
                 'rts_uid'              : None,
                 'parent_stage'         : {'uid': None, 'name': None},
                 'parent_pipeline'      : {'uid': None, 'name': None}
                }

    # FIXME: this should be converted into an RU/RS Attribute object, almost all
    #        of the code is redundant with the attribute class...

    def __init__(self):

        super(Task, self).__init__()

        self.gpu_reqs = {'processes'           : 1,
                         'process_type'        : None,
                         'threads_per_process' : 1,
                         'thread_type'         : None}
    @property
    def gpu_reqs(self):
        '''
        **Purpose:** The GPU requirements of the current Task.
        The requirements are described in terms of the number of processes and
        threads to be run in this Task. The expected format is:
        .. highlight:: python
        .. code-block:: python
            task.gpu_reqs = {'gpu_processes'    : X,
                             'gpu_process_type' : None/MPI,
                             'gpu_threads'      : Y,
                             'gpu_thread_type'  : None/OpenMP/CUDA}
        This description means that the Task is going to spawn X processes and
        Y threads per each of these processes to run on GPUs. Hence, the total
        number of gpus required by the Task is X*Y for all the processes and
        threads to execute concurrently. The same assumption is made in
        implementation and X*Y gpus are requested for this Task.
        The default value is:
        .. highlight:: python
        .. code-block:: python
            task.gpu_reqs = {'gpu_processes'    : 0,
                             'gpu_process_type' : None,
                             'gpu_threads'      : 0,
                             'gpu_thread_type'  : None}
        This description requests 0 gpus as not all machines have GPUs.
        :getter: return the gpu requirement of the current Task
        :setter: assign the gpu requirement of the current Task
        :arguments: dict
        '''
        tmp_val = dict()
        tmp_val['gpu_processes'] = self.gpu_reqs['processes']
        tmp_val['gpu_process_type'] = self.gpu_reqs['process_type']
        tmp_val['gpu_threads'] = self.gpu_reqs['threads_per_process']
        tmp_val['gpu_thread_type'] = self.gpu_reqs['thread_type']

        return tmp_val

    @gpu_reqs.setter
    def gpu_reqs(self, value):

        if not isinstance(value, dict):
            raise ree.TypeError(expected_type=dict, actual_type=type(value))

        # Deprecated keys will issue a deprecation message and change them to
        # the expected.
        depr_expected_keys = set(['processes', 'threads_per_process',
                             'process_type', 'thread_type'])

        expected_keys = set(['gpu_processes', 'gpu_threads',
                             'gpu_process_type', 'gpu_thread_type'])

        if set(value.keys()).issubset(depr_expected_keys):
            import warnings
            warnings.simplefilter("once")
            warnings.warn("GPU requirements keys are renamed using 'gpu_'" +
                           "as a prefix for all keys.",DeprecationWarning)

            value['gpu_processes'] = value.pop('processes')
            value['gpu_process_type'] = value.pop('process_type')
            value['gpu_threads'] = value.pop('threads_per_process')
            value['gpu_thread_type'] = value.pop('thread_type')

        missing = expected_keys - set(value.keys())

        if missing:
            raise ree.MissingError(obj='gpu_reqs', missing_attribute=missing)

        if not isinstance(value.get('gpu_processes'), (type(None), int)):
            raise ree.TypeError(expected_type=dict,
                                actual_type=type(value.get('gpu_processes')),
                                entity='gpu_processes')

        if value.get('gpu_process_type') not in [None, 'MPI', '']:
            raise ree.ValueError(expected_value='None or MPI',
                                 actual_value=value.get('gpu_process_type'),
                                 obj='gpu_reqs',
                                 attribute='gpu_process_type')

        if not isinstance(value.get('gpu_threads'), (type(None), int)):
            raise ree.TypeError(expected_type=int,
                             actual_type=type(value.get('gpu_threads')),
                             entity='gpu_threads')

        if value.get('gpu_thread_type') not in [None, 'OpenMP', 'CUDA','']:
            raise ree.ValueError(expected_value='None or OpenMP or CUDA',
                                 actual_value=value.get('gpu_thread_type'),
                                 obj='gpu_reqs',
                                 attribute='gpu_thread_type')

        self.gpu_reqs['processes']           = value.get('gpu_processes', 1)
        self.gpu_reqs['process_type']        = value.get('gpu_process_type')
        self.gpu_reqs['threads_per_process'] = value.get('gpu_threads', 1)
        self.gpu_reqs['thread_type']         = value.get('gpu_thread_type')
