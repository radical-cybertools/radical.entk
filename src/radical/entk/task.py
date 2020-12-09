
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
               'state'                : res,
               'state_history'        : [res],
               'pre_exec'             : [str],
               'executable'           : str,
               'arguments'            : [str],
               'sandbox'              : str,
               'post_exec'            : [str],
               'cpu_reqs'             : {str: int, str: str, str: int, str: str},
               'gpu_reqs'             : {str: int, str: str, str: int, str: str},
               'lfs_per_process'      : int,
               'upload_input_data'    : list(),
               'copy_input_data'      : list(),
               'link_input_data'      : list(),
               'move_input_data'      : list(),
               'copy_output_data'     : list(),
               'link_output_data'     : list(),
               'move_output_data'     : list(),
               'download_output_data' : list(),
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
                 'cpu_reqs'             : {'processes'           : 1,
                                           'process_type'        : None,
                                           'threads_per_process' : 1,
                                           'thread_type'         : None},
                 'gpu_reqs'             : {'processes'           : 1,
                                           'process_type'        : None,
                                           'threads_per_process' : 1,
                                           'thread_type'         : None},
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
                 'exit_code'            : None,
                 'path'                 : '',
                 'tag'                  : None,
                 'rts_uid'              : None,
                 'parent_stage'         : {'uid': None, 'name': None},
                 'parent_pipeline'      : {'uid': None, 'name': None}
                }

    # FIXME: this should be converted into an RU/RS Attribute object, almost all
    #        of the code is redundant with the attribute class...
