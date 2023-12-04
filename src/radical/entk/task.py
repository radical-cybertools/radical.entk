# pylint: disable=protected-access

__copyright__ = 'Copyright 2014-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import warnings

from string import punctuation
from typing import Dict, List, Union

import radical.utils as ru

from .constants  import NAME_MESSAGE
from .exceptions import EnTKError, EnTKMissingError
from .exceptions import EnTKTypeError, EnTKValueError
from .states     import INITIAL, _task_state_values

VALID_TAGS = ['colocate', 'exclusive']


# ------------------------------------------------------------------------------
#
class ReqsMixin:

    # --------------------------------------------------------------------------
    #
    def _correct_deprecated_key(self, k):
        if k in self._deprecated_keys:
            warnings.warn('Key "%s" is obsolete, new key ' % k +
                          '"%s" will be used' % self._deprecated_keys[k],
                          DeprecationWarning, stacklevel=2)
            k = self._deprecated_keys[k]
        return k

    # --------------------------------------------------------------------------
    #
    def __setitem__(self, k, v):
        k = self._correct_deprecated_key(k)
        super().__setitem__(k, v)

    # --------------------------------------------------------------------------
    #
    def __setattr__(self, k, v):
        k = self._correct_deprecated_key(k)
        super().__setattr__(k, v)


# ------------------------------------------------------------------------------
#
class CpuReqs(ReqsMixin, ru.TypedDict):

    _check = True

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

    # deprecated keys will cause a deprecation warning (ReqsMixin), and
    # correct keys, according to CpuReqs schema, will be provided
    _deprecated_keys = {
        'processes'          : 'cpu_processes',
        'process_type'       : 'cpu_process_type',
        'threads_per_process': 'cpu_threads',
        'thread_type'        : 'cpu_thread_type'
    }


# ------------------------------------------------------------------------------
#
class GpuReqs(ReqsMixin, ru.TypedDict):

    _check = True

    _schema = {
        'gpu_processes'   : float,
        'gpu_process_type': str,
        'gpu_threads'     : int,
        'gpu_thread_type' : str
    }

    _defaults = {
        'gpu_processes'   : 0.,
        'gpu_process_type': None,
        'gpu_threads'     : 1,
        'gpu_thread_type' : None
    }

    # deprecated keys will cause a deprecation warning (ReqsMixin), and
    # correct keys, according to GpuReqs schema, will be provided
    _deprecated_keys = {
        'processes'          : 'gpu_processes',
        'process_type'       : 'gpu_process_type',
        'threads_per_process': 'gpu_threads',
        'thread_type'        : 'gpu_thread_type'
    }


# ------------------------------------------------------------------------------
#
class Annotations(ru.TypedDict):

    _schema = {
        'inputs'    : [str],
        'outputs'   : [str],
        'depends_on': [str]
    }

    _defaults = {
        'inputs'    : [],
        'outputs'   : [],
        'depends_on': []
    }


# ------------------------------------------------------------------------------
#
class Task(ru.TypedDict):
    """
    A Task is an abstraction of a computational unit. In this case, a Task
    consists of its executable along with its required software environment,
    files to be staged as input and output.

    At the user level a Task is to be populated by assigning attributes.
    Internally, an empty Task is created and population using the `from_dict`
    function. This is to avoid creating Tasks with new `uid` as tasks with new
    `uid` offset the uid count file in radical.utils and can potentially affect
    the profiling if not taken care.

    .. data:: uid

        [type: `str` | default: `""`] A unique ID for the task. This attribute
        is optional, a unique ID will be assigned by RE if the field is not set,
        and cannot be re-assigned (immutable attribute).

    .. data:: name

        [type: `str` | default: `""`] A descriptive name for the task. This
        attribute can be used to map individual tasks back to application level
        workloads. Such characters as ',' or '_' are not allowed in a name.

    .. data:: state

        [type: `str` | default: `"DESCRIBED"`] Current state of the task, its
        initial value `re.states.INITIAL` is set during the task initialization,
        and this value is also appended into `state_history` attribute.

    .. data:: state_history

        [type: `list` | default: `["DESCRIBED"]`] List of the states obtained
        in temporal order. Every time new state is set, it is appended to
        `state_history` automatically.

    .. data:: executable

        [type: `str` | default: `""`] The executable to launch. The executable
        is expected to be either available via `$PATH` on the target resource,
        or to be an absolute path.

    .. data:: arguments

        [type: `list` | default: `[]`] List of arguments to be supplied to the
        `executable`.

    .. data:: environment

        [type: `dict` | default: `{}`] Environment variables to set in the
       environment before the execution process.

    .. data:: sandbox

        [type: `str` | default: `""`] This specifies the working directory of
        the task. By default, the task's uid is used as a sandbox for the task.

    .. data:: pre_launch

        [type: `list` | default: `[]`] List of actions (shell commands) to
        perform before the task is launched.

        Note that the set of shell commands given here are expected to load
        environments, check for work directories and data, etc. They are not
        expected to consume any significant amount of resources! Deviating from
        that rule will likely result in reduced overall throughput.

    .. data:: post_launch

        [type: `list` | default: `[]`] List of actions (shell commands) to
        perform after the task finishes.

        The same remarks as on `pre_launch` apply

    .. data:: pre_exec

        [type: `list` | default: `[]`] List of actions (shell commands) to
        perform after task is launched, but before rank(s) starts execution.

        The same remarks as on `pre_launch` apply

    .. data:: post_exec

        [type: `list` | default: `[]`] List of actions (shell commands) to
        perform right after rank(s) finishes.

        The same remarks as on `pre_launch` apply

    .. data:: cpu_reqs

        [type: `CpuReqs` | default: `CpuReqs()`] The CPU requirements for the
        current Task. The requirements are described in terms of the number of
        processes and threads to be run in this Task.

        The expected format is dict-like:

            task.cpu_reqs = {'cpu_processes'    : X,
                             'cpu_process_type' : None/'MPI',
                             'cpu_threads'      : Y,
                             'cpu_thread_type'  : None/'OpenMP'}

        This description means that the Task is going to spawn X processes and
        Y threads per each of these processes to run on CPUs. Hence, the total
        number of cpus required by the Task is `X * Y` for all the processes
        and threads to execute concurrently.

        By default, 1 CPU process and 1 CPU thread per process are requested.

    .. data:: gpu_reqs

        [type: `GpuReqs` | default: `GpuReqs()`] The GPU requirements for the
        current Task. The requirements are described in terms of the number of
        processes and threads to be run in this Task.

        The expected format is dict-like:

            task.gpu_reqs = {'gpu_processes'    : X,
                             'gpu_process_type' : None/'CUDA'/'ROCm',
                             'gpu_threads'      : Y,
                             'gpu_thread_type'  : None}

        This description means that each rank of the task is going to use X GPUs
        with Y GPU-threads.  By default, 0 GPUs are requested.

    .. data:: lfs_per_process

        [type: `int` | default: `0`] Local File Storage per process - amount of
        space (MB) required on the local file system of the node by the task.

    .. data:: mem_per_process

        [type: `int` | default: `0`] Amount of memory required by the task.

    .. data:: upload_input_data

        [type: `list` | default: `[]`] List of file names required to be
        transferred from a local client storage to the location of the task or
        data staging area before task starts.

    .. data:: copy_input_data

        [type: `list` | default: `[]`] List of file names required to be copied
        from another task-/pilot-sandbox to a current task (or data staging
        area) before task starts.

        Example of a file location format:

            $Pipeline_%s_Stage_%s_Task_%s, where %s is replaced entity name/uid

            # output.txt is copied from a t1 task to a current task sandbox
            t2.copy_input_data = ['$Pipeline_%s_Stage_%s_Task_%s/output.txt' %
                                  (p.name, s1.name, t1.name)]

    .. data:: link_input_data

        [type: `list` | default: `[]`] List of file names required to be
        symlinked from another task-/pilot-sandbox to a current task (or data
        staging area) before task starts.

    .. data:: move_input_data

        [type: `list` | default: `[]`] List of file names required to be moved
        from another task-/pilot-sandbox to a current task (or data staging
        area) before task starts.

    .. data:: copy_output_data

        [type: `list` | default: `[]`] List of file names required to be copied
        from a current task to another task-/pilot-sandbox (or data staging
        area) when a task is finished.

        Example of defining data to be copied:

            # results.txt will be copied to a data staging area `$SHARED`
            t.copy_output_data = ['results.txt > $SHARED/results.txt']

    .. data:: link_output_data

        [type: `list` | default: `[]`] List of file names required to be
        symlinked from a current task to another task-/pilot-sandbox (or data
        staging area) when a task is finished.

    .. data:: move_output_data

        [type: `list` | default: `[]`] List of file names required to be moved
        from a current task to another task-/pilot-sandbox (or data staging
        area) when a task is finished.

    .. data:: download_output_data

        [type: `list` | default: `[]`] List of file names required to be
        downloaded from a current task to a local client storage space when a
        task is finished.

        Example of defining data to be downloaded:

            # results.txt is transferred to a local client storage space
            t.download_output_data = ['results.txt']

    .. data:: stdout

        [type: `str` | default: `""`] The name of the file to store stdout. If
        not set then the name of the following format will be used: `<uid>.out`.

    .. data:: stderr

        [type: `str` | default: `""`] The name of the file to store stderr. If
        not set then the name of the following format will be used: `<uid>.err`.

    .. data:: stage_on_error

        [type: `bool` | default: `False`] Flag to allow staging out data if
        task got failed (output staging is normally skipped on `FAILED` or
        `CANCELED` tasks).

    .. data:: exit_code

        [type: `int` | default: `None`] Get the exit code for finished tasks:
        0 - for successful tasks; 1 - for failed tasks.

    .. data:: exception

        [type: `str` | default: `None`] Get the representation of the exception
        which caused the task to fail.

    .. data:: exception_detail

        [type: `str` | default: `None`] Get additional details (traceback or
        error messages) to the exception which caused this task to fail.

    .. data:: path

        [type: `str` | default: `""`] Get the path of the task on the remote
        machine. Useful to reference files generated in the current task.

    .. data:: tags

        [type: `dict` | default: `None`] The tags for the task that can be
        used while scheduling by the RTS (configuration specific tags, which
        influence task scheduling and execution, e.g., tasks co-location).

    .. data:: rts_uid

        [type: `str` | default: `None`] Unique RTS ID of the current task.

    .. data:: parent_stage

        [type: `dict` | default: `{'uid': None, 'name': None}`] Identification
        of the stage, which contains the current task.

    .. data:: parent_pipeline

        [type: `dict` | default: `{'uid': None, 'name': None}`] Identification
        of the pipeline, which contains the current task.

    .. data:: annotations

        [type: `Annotations` | default: `None`] Annotations to describe task's
        input and output files, and sets dependencies between tasks.

    Read-only attributes
    --------------------

    .. property:: luid

        [type: `str`] Unique ID of the current task (fully qualified).

            > task.luid
            pipe.0001.stage.0004.task.0234
    """

    _check = True
    _uids  = []

    _schema = {
        'uid'                  : str,
        'name'                 : str,
        'state'                : str,
        'state_history'        : [str],
        'executable'           : str,
        'arguments'            : [str],
        'environment'          : {str: str},
        'sandbox'              : str,
        'pre_launch'           : [str],
        'post_launch'          : [str],
        'pre_exec'             : [str],
        'post_exec'            : [str],
        'cpu_reqs'             : CpuReqs,
        'gpu_reqs'             : GpuReqs,
        'lfs_per_process'      : int,
        'mem_per_process'      : int,
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
        'exception'            : str,
        'exception_detail'     : str,
        'path'                 : str,
        'tags'                 : {str: None},
        'rts_uid'              : str,
        'parent_stage'         : {str: None},
        'parent_pipeline'      : {str: None},
        'annotations'          : Annotations
    }

    # guaranteed attributes with default non-initialized values
    _defaults = {
        'uid'                  : '',
        'name'                 : '',
        'state'                : '',
        'state_history'        : [],
        'executable'           : '',
        'arguments'            : [],
        'environment'          : {},
        'sandbox'              : '',
        'pre_launch'           : [],
        'post_launch'          : [],
        'pre_exec'             : [],
        'post_exec'            : [],
        'cpu_reqs'             : CpuReqs(),
        'gpu_reqs'             : GpuReqs(),
        'lfs_per_process'      : 0,
        'mem_per_process'      : 0,
        'upload_input_data'    : [],
        'copy_input_data'      : [],
        'link_input_data'      : [],
        'move_input_data'      : [],
        'copy_output_data'     : [],
        'link_output_data'     : [],
        'move_output_data'     : [],
        'download_output_data' : [],
        'stdout'               : '',
        'stderr'               : '',
        'stage_on_error'       : False,
        'exit_code'            : None,
        'exception'            : None,
        'exception_detail'     : None,
        'path'                 : '',
        'tags'                 : None,
        'rts_uid'              : None,
        'parent_stage'         : {'uid': None, 'name': None},
        'parent_pipeline'      : {'uid': None, 'name': None}
    }

    # --------------------------------------------------------------------------
    #
    def __init__(self, from_dict=None):

        from_dict = from_dict or {}
        if not isinstance(from_dict, dict):
            raise EnTKTypeError(expected_type=dict, actual_type=type(from_dict))

        super().__init__()

        attrs = list(from_dict.keys())
        # `uid` is allowed to be set during initialization only
        if not from_dict.get('uid'):
            uid = ru.generate_id('task.%(counter)06d', ru.ID_CUSTOM)
        else:
            attrs.remove('uid')
            uid = super()._verify_setter('uid', from_dict['uid'])
            if any(s in uid for s in punctuation.replace('.', '')):
                raise EnTKError('Incorrect symbol for attribute "uid" (%s). %s'
                                % (uid, NAME_MESSAGE))

        self._data['uid'] = uid

        # ensure that "state" and "state_history" are handled correctly
        self['state'] = from_dict.get('state') or INITIAL
        if 'state' in from_dict:
            attrs.remove('state')
        if 'state_history' in from_dict:
            self['state_history'] = from_dict['state_history']
            attrs.remove('state_history')

        for attr in attrs:
            self[attr] = from_dict[attr]

    # --------------------------------------------------------------------------
    #
    def __eq__(self, other):

        if not isinstance(other, Task):
            return False

        return self['uid'] == other['uid']

    # --------------------------------------------------------------------------
    #
    def __ne__(self, other):

        return not self == other

    # --------------------------------------------------------------------------
    #
    def __hash__(self):

        return hash(self['uid'])

    # --------------------------------------------------------------------------
    #
    def _post_verifier(self, k, v):

        if not v:
            return

        if k == 'uid':
            raise EnTKError('Task.uid is not allowed to be re-assigned')

        elif k == 'name':
            if any(symbol in v for symbol in punctuation.replace('.', '')):
                raise EnTKError('Incorrect symbol for attribute "%s" (%s). %s'
                                % (k, v, NAME_MESSAGE))

        elif k == 'state':
            if v not in _task_state_values:
                raise EnTKValueError(obj=self['uid'], attribute=k,
                                    expected_value=list(_task_state_values),
                                    actual_value=v)
            self['state_history'].append(v)

        elif k == 'state_history':
            for _v in v:
                if _v not in _task_state_values:
                    raise EnTKValueError(obj=self['uid'],
                                    attribute='state_history element',
                                    expected_value=list(_task_state_values),
                                    actual_value=_v)

        elif k == 'tags':
            if any(tag not in VALID_TAGS for tag in v):
                raise EnTKError(
                    'Incorrect structure for attribute "%s" of object %s' %
                    (k, self['uid']))

    # --------------------------------------------------------------------------
    #
    def _verify_setter(self, k, v):
        v = super()._verify_setter(k, v)
        self._post_verifier(k, v)
        return v

    # --------------------------------------------------------------------------
    #
    def _verify(self):
        verify_attrs = list(self.keys())
        verify_attrs.remove('uid')

        for k in verify_attrs:
            self._post_verifier(k, self[k])

    # --------------------------------------------------------------------------
    #
    @property
    def luid(self):
        """
        Unique ID of the current task (fully qualified).

        Example:
            > task.luid
            pipe.0001.stage.0004.task.123456

        :luid: Returns the fully qualified uid of the current task
        :type: str
        """

        # TODO: cache

        p_elem = self['parent_pipeline'].get('name')
        s_elem = self['parent_stage'].get('name')
        t_elem = self['name']

        if not p_elem: p_elem = self['parent_pipeline']['uid']
        if not s_elem: s_elem = self['parent_stage']['uid']
        if not t_elem: t_elem = self['uid']

        return '%s.%s.%s' % (p_elem, s_elem, t_elem)

    # --------------------------------------------------------------------------
    #
    def annotate(self, inputs: Union[Dict, List, str, None] = None,
                       outputs: Union[List, str, None] = None) -> None:
        """
        Adds dataflow annotations with provided input and output files,
        and defines dependencies between tasks.

        Attributes:
            inputs (list, optional): List of input files. If a file is
                produced by the previously executed task, then the
                corresponding input element is provided as a dictionary with
                the task instance as a key.
                Example: inputs=['file1', {task0: 'file2', task1: ['file2']}]

            outputs (list, optional): List of produced/generated files.
        """
        ta = self.setdefault('annotations', Annotations())

        # TODO: have a unified file representation (file name within task
        #       sandbox or file full path, considering files outside of the
        #       task sandbox) - set an expand procedure. Ensure that it is
        #       the same for inputs and outputs

        if inputs:
            for i in ru.as_list(inputs):

                if isinstance(i, str):
                    ta.inputs.append(i)

                elif isinstance(i, dict):
                    for task, task_files in i.items():
                        if task.uid not in ta.depends_on:
                            ta.depends_on.append(task.uid)
                        for tf in ru.as_list(task_files):
                            # corresponding validation of dependencies
                            # is part of the Pipeline validation process
                            ta.inputs.append('%s:%s' % (task.uid, tf))

        if outputs:
            ta.outputs.extend(ru.as_list(outputs))
            if len(ta.outputs) != len(set(ta.outputs)):
                warnings.warn('Annotated outputs for %s ' % self['uid'] +
                              'includes duplication(s)')

    # --------------------------------------------------------------------------
    #
    def from_dict(self, d):
        """Re-initialization, resets all attributes with provided input data."""
        self.__init__(from_dict=d)

    # --------------------------------------------------------------------------
    #
    def _validate(self):
        """
        Purpose: Validate that the state of the task is 'DESCRIBED' and that an
        executable has been specified for the task.
        """

        if self['uid'] in Task._uids:
            raise EnTKError('Task ID %s already exists' % self['uid'])
        else:
            Task._uids.append(self['uid'])

        if self['state'] is not INITIAL:
            raise EnTKValueError(obj=self['uid'], attribute='state',
                                 expected_value=INITIAL,
                                 actual_value=self['state'])

        if not self['executable']:
            raise EnTKMissingError(obj=self['uid'],
                                   missing_attribute='executable')

# ------------------------------------------------------------------------------

