__copyright__ = 'Copyright 2014-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

from string import punctuation

import radical.utils as ru

from .constants import NAME_MESSAGE
from . import exceptions as ree
from . import states     as res


# ------------------------------------------------------------------------------
#
class CpuReqs(ru.Munch):

    _schema = {
        'cpu_processes'   : int,
        'cpu_process_type': str,
        'cpu_threads'     : int,
        'cpu_thread_type' : str
    }

    _defaults = {
        'cpu_processes'   : 1,
        'cpu_process_type': None,
        'cpu_threads'     : 1,
        'cpu_thread_type' : None
    }


# ------------------------------------------------------------------------------
#
class GpuReqs(ru.Munch):

    _schema = {
        'gpu_processes'   : int,
        'gpu_process_type': str,
        'gpu_threads'     : int,
        'gpu_thread_type' : str
    }

    _defaults = {
        'gpu_processes'   : 0,
        'gpu_process_type': None,
        'gpu_threads'     : 1,
        'gpu_thread_type' : None
    }


# ------------------------------------------------------------------------------
#
class Task(ru.Munch):
    """
    A Task is an abstraction of a computational unit. In this case, a Task
    consists of its executable along with its required software environment,
    files to be staged as input and output.

    At the user level a Task is to be populated by assigning attributes.
    Internally, an empty Task is created and population using the `from_dict`
    function. This is to avoid creating Tasks with new `uid` as tasks with new
    `uid` offset the uid count file in radical.utils and can potentially affect
    the profiling if not taken care.
    """

    _check = True
    _uids  = list()

    _schema = {
        'uid'                  : str,
        'name'                 : str,
        'state'                : str,
        'state_history'        : [str],
        'executable'           : str,
        'arguments'            : [str],
        'sandbox'              : str,
        'pre_exec'             : [str],
        'post_exec'            : [str],
        'cpu_reqs'             : CpuReqs,
        'gpu_reqs'             : GpuReqs,
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
        'stage_on_error'       : bool,
        'exit_code'            : int,
        'path'                 : str,
        'tags'                 : {str: str},
        'rts_uid'              : str,
        'parent_stage'         : {str: None},
        'parent_pipeline'      : {str: None}
    }

    _defaults = {
        'uid'                  : '',
        'name'                 : '',
        'state'                : res.INITIAL,
        'executable'           : '',
        'arguments'            : list(),
        'sandbox'              : '',
        'pre_exec'             : list(),
        'post_exec'            : list(),
        'cpu_reqs'             : CpuReqs._defaults,
        'gpu_reqs'             : GpuReqs._defaults,
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
        'stage_on_error'       : False,
        'exit_code'            : None,
        'path'                 : '',
        'tags'                 : None,
        'rts_uid'              : None,
        'parent_stage'         : {'uid': None, 'name': None},
        'parent_pipeline'      : {'uid': None, 'name': None}
    }

    # --------------------------------------------------------------------------
    #
    def __init__(self, from_dict=None):

        if from_dict and not isinstance(from_dict, dict):
            raise ree.TypeError(expected_type=dict,
                                actual_type=type(from_dict))

        super().__init__(from_dict=from_dict)

        if not self.uid:
            self.uid = ru.generate_id('task.%(counter)04d', ru.ID_CUSTOM)

    # --------------------------------------------------------------------------
    #
    def _post_setter(self, k, v):

        if not v:
            return

        if k in ['uid', 'name']:
            invalid_symbols = punctuation.replace('.', '')
            if any(symbol in v for symbol in invalid_symbols):
                raise ree.EnTKError(
                    'Incorrect symbol for attribute "%s" of object %s. %s' %
                    (k, self.uid, NAME_MESSAGE))

        elif k == 'state':
            if v not in res._task_state_values:
                raise ree.ValueError(
                    obj=self.uid,
                    attribute=k,
                    expected_value=list(res._task_state_values),
                    actual_value=v)
            if 'state_history' not in self:
                self['state_history'] = []
            self['state_history'].append(v)

        elif k == 'state_history':
            for _v in v:
                if _v not in res._task_state_values:
                    raise ree.ValueError(
                        obj=self.uid,
                        attribute='state_history element',
                        expected_value=list(res._task_state_values),
                        actual_value=_v)

        elif k == 'tags':
            if list(v) != ['colocate']:
                raise ree.EnTKError(
                    'Incorrect structure for attribute "%s" of object %s' %
                    (k, self.uid))

    # --------------------------------------------------------------------------
    #
    def __setitem__(self, k, v):
        super().__setitem__(k, v)
        self._post_setter(k, v)

    # --------------------------------------------------------------------------
    #
    def __setattr__(self, k, v):
        super().__setattr__(k, v)
        self._post_setter(k, v)

    # --------------------------------------------------------------------------
    #
    @property
    def luid(self):
        """
        Unique ID of the current task (fully qualified).
        example:
            > task.luid
            pipe.0001.stage.0004.task.0234
        :luid: Returns the fully qualified uid of the current task
        :type: String
        """

        # TODO: cache

        p_elem = self.parent_pipeline.get('name')
        s_elem = self.parent_stage.get('name')
        t_elem = self.name

        if not p_elem: p_elem = self.parent_pipeline['uid']
        if not s_elem: s_elem = self.parent_stage['uid']
        if not t_elem: t_elem = self.uid

        return '%s.%s.%s' % (p_elem, s_elem, t_elem)


    # --------------------------------------------------------------------------
    #
    def to_dict(self):
        return self.as_dict()

    # --------------------------------------------------------------------------
    #
    def from_dict(self, d):

        self.update(d)
        if 'state' in d:
            # avoid adding state to state history
            del self.state_history[-1]

    # --------------------------------------------------------------------------
    #
    def _validate(self):
        """
        Purpose: Validate that the state of the task is 'DESCRIBED' and that an
        executable has been specified for the task.
        """

        if self.uid in Task._uids:
            raise ree.EnTKError(msg='Task ID %s already exists' % self.uid)
        else:
            Task._uids.append(self.uid)

        if self.state is not res.INITIAL:
            raise ree.ValueError(obj=self.uid,
                                 attribute='state',
                                 expected_value=res.INITIAL,
                                 actual_value=self.state)

        if not self.executable:
            raise ree.MissingError(obj=self.uid,
                                   missing_attribute='executable')

# ------------------------------------------------------------------------------
