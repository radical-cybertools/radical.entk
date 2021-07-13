__copyright__ = 'Copyright 2014-2020, http://radical.rutgers.edu'
__license__   = 'MIT'

import warnings

import radical.utils as ru

from string     import punctuation
from .constants import NAME_MESSAGE

from . import exceptions as ree
from . import states     as res

warnings.simplefilter(action="once", category=DeprecationWarning, lineno=728)
warnings.simplefilter(action="once", category=DeprecationWarning, lineno=786)
warnings.simplefilter(action="once", category=DeprecationWarning, lineno=954)


# ------------------------------------------------------------------------------
#
class Task(object):
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


    _uids = list()

    # FIXME: this should be converted into an RU/RS Attribute object, almost all
    #        of the code is redundant with the attribute class...

    # --------------------------------------------------------------------------
    #
    def __init__(self, from_dict=None):

        self._uid     = ru.generate_id('task.%(counter)04d', ru.ID_CUSTOM)
        self._name    = ""
        self._state   = res.INITIAL
        self._rts_uid = None

        # Attributes necessary for execution
        self._pre_exec   = list()
        self._executable = ""
        self._arguments  = list()
        self._sandbox    = ""
        self._post_exec  = list()
        self._stage_on_error = False

        self._lfs_per_process = 0
        self._cpu_reqs        = {'processes'           : 1,
                                 'process_type'        : None,
                                 'threads_per_process' : 1,
                                 'thread_type'         : None}
        self._gpu_reqs        = {'processes'           : 0,
                                 'process_type'        : None,
                                 'threads_per_process' : 0,
                                 'thread_type'         : None}

        # Data staging attributes
        self._upload_input_data    = list()
        self._copy_input_data      = list()
        self._link_input_data      = list()
        self._move_input_data      = list()
        self._copy_output_data     = list()
        self._link_output_data     = list()
        self._move_output_data     = list()
        self._download_output_data = list()

        # Name of file to write stdout and stderr of task
        self._stdout = ""
        self._stderr = ""

        # Additional attributes that help in mapping tasks
        # to cuds and cus to tasks
        self._path      = None
        self._exit_code = None
        self._tags      = None

        # Keep track of res attained
        self._state_history = [res.INITIAL]

        # Stage and pipeline this task belongs to
        self._p_stage    = {'uid': None, 'name': None}
        self._p_pipeline = {'uid': None, 'name': None}

        # populate task attributes if so requesteed
        if from_dict:

            if not isinstance(from_dict, dict):
                raise ree.TypeError(expected_type=dict,
                                    actual_type=type(from_dict))

            for k,v in from_dict.items():
                self.__setattr__(k, v)


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        '''
        Unique ID of the current task

        :getter: Returns the unique id of the current task
        :type: String
        '''
        return self._uid


    @property
    def luid(self):
        '''
        Unique ID of the current task (fully qualified).

        example:
            >>> task.luid
            pipe.0001.stage.0004.task.0234

        :getter: Returns the fully qualified uid of the current task
        :type: String
        '''

        # TODO: cache


        p_elem = self.parent_pipeline.get('name')
        s_elem = self.parent_stage.get('name')
        t_elem = self.name

        if not p_elem: p_elem = self.parent_pipeline['uid']
        if not s_elem: s_elem = self.parent_stage['uid']
        if not t_elem: t_elem = self.uid

        return '%s.%s.%s' % (p_elem, s_elem, t_elem)


    @property
    def name(self):
        '''
        Name of the task. Do not use a ',' or '_' in an object's name.

        :getter: Returns the name of the current task
        :setter: Assigns the name of the current task
        :type: String
        '''

        return self._name


    @property
    def state(self):
        '''
        Current state of the task

        :getter: Returns the state of the current task
        :type: String
        '''

        return self._state


    @property
    def pre_exec(self):
        '''
        List of commands to be executed prior to the executable

        :getter: return the list of commands
        :setter: assign the list of commands
        :arguments: list of strings
        '''

        return self._pre_exec


    @property
    def executable(self):
        '''
        A unix-based kernel to be executed

        :getter: returns the executable of the current task
        :setter: assigns the executable for the current task
        :arguments: string
        '''

        return self._executable


    @property
    def arguments(self):
        '''
        List of arguments to be supplied to the executable

        :getter: returns the list of arguments of the current task
        :setter: assigns a list of arguments to the current task
        :arguments: list of strings
        '''

        return self._arguments


    @property
    def sandbox(self):
        '''
        Sandbox the task is running in

        :getter: returns the sandbox name
        :setter: assigns the sandbox name
        :arguments: string
        '''

        return self._sandbox


    @property
    def post_exec(self):
        '''
        List of commands to be executed post executable

        :getter: return the list of commands
        :setter: assign the list of commands
        :arguments: list of strings
        '''

        return self._post_exec


    @property
    def cpu_reqs(self):
        '''
        **Purpose:** The CPU requirements of the current Task.

        The requirements are described in terms of the number of processes and
        threads to be run in this Task. The expected format is:

        .. highlight:: python
        .. code-block:: python

            task.cpu_reqs = {'cpu_processes'    : X,
                             'cpu_process_type' : None/MPI,
                             'cpu_threads'      : Y,
                             'cpu_thread_type'  : None/OpenMP}

        This description means that the Task is going to spawn X processes and
        Y threads per each of these processes to run on CPUs. Hence, the total
        number of cpus required by the Task is X*Y for all the processes and
        threads to execute concurrently. The same assumption is made in
        implementation and X*Y cpus are requested for this Task.

        The default value is:

        .. highlight:: python
        .. code-block:: python

            task.cpu_reqs = {'cpu_processes'    : 1,
                             'cpu_process_type' : None,
                             'cpu_threads'      : 1,
                             'cpu_thread_type'  : None}

        This description requests 1 core and expected the executable to non-MPI
        and single threaded.

        :getter: return the cpu requirement of the current Task
        :setter: assign the cpu requirement of the current Task
        :arguments: dict
        '''
        tmp_val = dict()
        tmp_val['cpu_processes'] = self._cpu_reqs['processes']
        tmp_val['cpu_process_type'] = self._cpu_reqs['process_type']
        tmp_val['cpu_threads'] = self._cpu_reqs['threads_per_process']
        tmp_val['cpu_thread_type'] = self._cpu_reqs['thread_type']

        return tmp_val


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
        tmp_val['gpu_processes'] = self._gpu_reqs['processes']
        tmp_val['gpu_process_type'] = self._gpu_reqs['process_type']
        tmp_val['gpu_threads'] = self._gpu_reqs['threads_per_process']
        tmp_val['gpu_thread_type'] = self._gpu_reqs['thread_type']

        return tmp_val


    @property
    def lfs_per_process(self):
        '''
        Set the amount of local file-storage space required by the task
        '''

        return self._lfs_per_process


    @property
    def upload_input_data(self):
        '''
        Transfers data (filenames in a list) from a local client (e.g. laptop)
        to the location of the current task (or data staging area) before it
        starts.

        :getter: return the list of files
        :setter: assign the list of files
        :arguments: list of strings
        '''

        return self._upload_input_data


    @property
    def copy_input_data(self):
        '''
        Copies data (filenames in a list) from another task to a current task
        (or data staging area) before it starts.

        The following is an example of how to locate the file with a certain
        format, $Pipeline_%s_Stage_%s_Taask_%s, where %s is replaced entity name
        or uid:

        .. highlight:: python

        .. code-block:: python

            t2.copy_input_data = ['$Pipeline_%s_Stage_%s_Task_%s/output.txt' %
                                  (p.name, s1.name, t1.name)]
            # output.txt is copied from a t1 task to a current task before it
            # starts.

        :getter: return the list of files
        :setter: assign the list of files
        :arguments: list of strings
        '''

        return self._copy_input_data


    @property
    def link_input_data(self):
        '''
        Symlinks data (filenames in a list) from another task to a current task
        (or data staging area) before it starts.

        :getter: return the list of files
        :setter: assign the list of files
        :arguments: list of strings
        '''

        return self._link_input_data


    @property
    def move_input_data(self):
        '''
        Moves data (filenames in a list) from another task to a current task
        (or data staging area) before it starts.

        :getter: return the list of files
        :setter: assign the list of files
        :arguments: list of strings
        '''

        return self._move_input_data


    @property
    def copy_output_data(self):
        '''
        Copies data (filenames in a list) from a current task to another task
        (or data staging area) when a task is finished.

        The following is an example

        .. highlight:: python

        .. code-block:: python

            t.copy_output_data = [ 'results.txt > $SHARED/results.txt' ]
            # results.txt is copied to a data staging area `$SHARED` when a task is
            # finised.

        :getter: return the list of files
        :setter: assign the list of files
        :arguments: list of strings
        '''

        return self._copy_output_data


    @property
    def link_output_data(self):
        '''
        Symlins data (filenames in a list) from a current task to another task
        (or data staging area) when a task is finished.

        :getter: return the list of files
        :setter: assign the list of files
        :arguments: list of strings
        '''

        return self._link_output_data


    @property
    def move_output_data(self):
        '''
        Moves data (filenames in a list) from a current task to another task
        (or data staging area) when a task is finished.

        :getter: return the list of files
        :setter: assign the list of files
        :arguments: list of strings
        '''

        return self._move_output_data


    @property
    def download_output_data(self):
        '''
        Downloads data (filenames in a list) from a current task to a local
        client (e.g. laptop) when a task is finished.

        The following is an example
        .. highlight:: python

        .. code-block:: python

            t.download_output_data = [ 'results.txt' ]
            # results.txt is transferred to a local client (e.g. laptop) when a
           # current task finised.

        :getter: return the list of files
        :setter: assign the list of files
        :arguments: list of strings
        '''

        return self._download_output_data


    @property
    def stdout(self):
        '''
        Name of the file to which stdout of task is to be written

        :getter: return name of stdout file
        :setter: assign name of stdout file
        :arguments: str
        '''

        return self._stdout


    @property
    def stderr(self):
        '''
        Name of the file to which stderr of task is to be written

        :getter: return name of stderr file
        :setter: assign name of stderr file
        :arguments: str
        '''

        return self._stderr


    @property
    def exit_code(self):
        '''
        Get the exit code for DONE tasks. 0 for successful, 1 for failed tasks.

        :getter: return the exit code of the current task
        '''

        return self._exit_code


    @property
    def path(self):
        '''
        Get the path of the task on the remote machine. Useful to reference
        files generated in the current task.

        :getter: return the path of the current task
        '''

        return self._path


    @property
    def tag(self):
        '''
        WARNING: It will be deprecated.
        '''

        return self._tags


    @property
    def tags(self):
        '''
        Set the tags for the task that can be used while scheduling by the RTS

        :getter: return the tags of the current task
        '''

        return self._tags


    @property
    def parent_stage(self):
        '''
        :getter: Returns the stage this task belongs to
        :setter: Assigns the stage uid this task belongs to
        '''

        return self._p_stage


    @property
    def parent_pipeline(self):
        '''
        :getter: Returns the pipeline this task belongs to
        :setter: Assigns the pipeline uid this task belongs to
        '''

        return self._p_pipeline


    @property
    def state_history(self):
        '''
        Returns a list of the states obtained in temporal order

        :return: list
        '''

        return self._state_history

    @property
    def rts_uid(self):
        '''
        Unique RTS ID of the current task

        :getter: Returns the RTS unique id of the current task
        :type: String
        '''

        return self._rts_uid


    @property
    def stage_on_error(self):
        '''
        Allow to stage out data if task failed

        :getter: Returns the value
        :type: Boolean
        '''

        return self._stage_on_error
    # --------------------------------------------------------------------------
    #
    @uid.setter
    def uid(self, value):
        invalid_symbols = punctuation.replace('.','')
        if not isinstance(value, str):
            raise ree.TypeError(expected_type=str,
                                actual_type=type(value))

        if any(symbol in value for symbol in invalid_symbols):
            raise ree.ValueError(obj=self._uid,
                                 attribute='uid',
                                 actual_value=value,
                                 expected_value=NAME_MESSAGE)
        self._uid = value

    @rts_uid.setter
    def rts_uid(self, value):

        if not isinstance(value, str):
            raise ree.TypeError(expected_type=str,
                                actual_type=type(value))

        self._rts_uid = value

    @name.setter
    def name(self, value):
        invalid_symbols = punctuation.replace('.','')
        if not isinstance(value, str):
            raise ree.TypeError(expected_type=str,
                                actual_type=type(value))

        if any(symbol in value for symbol in invalid_symbols):
            raise ree.ValueError(obj=self._uid,
                                 attribute='name',
                                 actual_value=value,
                                 expected_value=NAME_MESSAGE)

        self._name = value


    @state.setter
    def state(self, value):

        # FIXME: this state setter has side effects (adding to state_history)
        #        which should be moved into a separate method.  Also, a state
        #        progression check is missing to ensure the state model

        if not isinstance(value, str):
            raise ree.TypeError(expected_type=str,
                                actual_type=type(value))

        if value not in res._task_state_values:
            raise ree.ValueError(obj=self._uid,
                             attribute='state',
                             expected_value=list(res._task_state_values.keys()),
                             actual_value=value)
        self._state = value
        self._state_history.append(value)


    @state_history.setter
    def state_history(self, value):

        if not isinstance(value, list):
            raise ree.TypeError(entity='state_history', expected_type=list,
                                actual_type=type(value))

        for elem in value:
            if elem not in res._task_state_values:
                raise ree.ValueError(obj=self._uid,
                                 attribute='state_history element',
                                 expected_value=list(res._task_state_values.keys()),
                                 actual_value=elem)
        self._state_history = value


    @pre_exec.setter
    def pre_exec(self, value):

        if not isinstance(value, list):
            raise ree.TypeError(expected_type=list, actual_type=type(value))

        self._pre_exec = value


    @executable.setter
    def executable(self, value):

        if not isinstance(value, str):
            raise ree.TypeError(expected_type='str',
                                actual_type=type(value))

        self._executable = value


    @arguments.setter
    def arguments(self, value):

        if not isinstance(value, list):
            raise ree.TypeError(expected_type=list, actual_type=type(value))

        self._arguments = value


    @sandbox.setter
    def sandbox(self, value):

        if not isinstance(value, str):
            raise ree.TypeError(expected_type=str, actual_type=type(value))

        self._sandbox = value


    @post_exec.setter
    def post_exec(self, value):

        if not isinstance(value, list):
            raise ree.TypeError(expected_type=list, actual_type=type(value))

        self._post_exec = value


    @cpu_reqs.setter
    def cpu_reqs(self, value):
        if not isinstance(value, dict):
            raise ree.TypeError(expected_type=dict, actual_type=type(value))

        # Deprecated keys will issue a deprecation message and change them to
        # the expected.
        depr_expected_keys = set(['processes', 'threads_per_process',
                             'process_type', 'thread_type'])

        expected_keys = set(['cpu_processes', 'cpu_threads',
                             'cpu_process_type', 'cpu_thread_type'])

        if set(value.keys()).issubset(depr_expected_keys):
            warnings.warn("CPU requirements keys are renamed. Please use " +
                          "cpu_processes for processes, cpu_process_type for " +
                          "process_type, cpu_threads for threads_per_process " +
                          "and cpu_thread_type for thread_type",
                          DeprecationWarning, stacklevel=2)

            value['cpu_processes'] = value.pop('processes')
            value['cpu_process_type'] = value.pop('process_type')
            value['cpu_threads'] = value.pop('threads_per_process')
            value['cpu_thread_type'] = value.pop('thread_type')

        missing = expected_keys - set(value.keys())

        if missing:
            raise ree.MissingError(obj='cpu_reqs', missing_attribute=missing)

        if not isinstance(value.get('cpu_processes'), (type(None), int)):
            raise ree.TypeError(expected_type=int,
                                actual_type=type(value.get('cpu_processes')),
                                entity='cpu_processes')

        if value.get('cpu_process_type') not in [None, 'MPI', '']:
            raise ree.ValueError(expected_value='None or MPI',
                                 actual_value=value.get('cpu_process_type'),
                                 obj='cpu_reqs',
                                 attribute='cpu_process_type')

        if not isinstance(value.get('cpu_threads'), (type(None), int)):
            raise ree.TypeError(expected_type=int,
                             actual_type=type(value.get('cpu_threads')),
                             entity='cpu_threads')

        if value.get('cpu_thread_type') not in [None, 'OpenMP', '']:
            raise ree.ValueError(expected_value='None or OpenMP', obj='cpu_reqs',
                                 actual_value=value.get('cpu_thread_type'),
                                 attribute='cpu_thread_type')

        self._cpu_reqs['processes']           = value.get('cpu_processes', 1)
        self._cpu_reqs['process_type']        = value.get('cpu_process_type')
        self._cpu_reqs['threads_per_process'] = value.get('cpu_threads', 1)
        self._cpu_reqs['thread_type']         = value.get('cpu_thread_type')


    @gpu_reqs.setter
    def gpu_reqs(self, value):

        if not isinstance(value, dict):
            raise ree.TypeError(expected_type=dict, actual_type=type(value))

        # Deprecated keys will issue a deprecation message and change them to
        # the expected.
        depr_expected_keys = {'processes', 'threads_per_process',
                              'process_type', 'thread_type'}

        expected_keys = {'gpu_processes', 'gpu_threads', 'gpu_process_type',
                         'gpu_thread_type'}

        if set(value.keys()).issubset(depr_expected_keys):
            warnings.warn("GPU requirements keys are renamed. Please use " +
                          "gpu_processes for processes, gpu_process_type for " +
                          "process_type, gpu_threads for threads_per_process " +
                          "and gpu_thread_type for thread_type",
                          DeprecationWarning, stacklevel=2)

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

        self._gpu_reqs['processes']           = value.get('gpu_processes', 1)
        self._gpu_reqs['process_type']        = value.get('gpu_process_type')
        self._gpu_reqs['threads_per_process'] = value.get('gpu_threads', 1)
        self._gpu_reqs['thread_type']         = value.get('gpu_thread_type')


    @lfs_per_process.setter
    def lfs_per_process(self, value):

        if not isinstance(value, int):
            raise ree.TypeError(expected_type=int, actual_type=type(value))

        self._lfs_per_process = value


    @upload_input_data.setter
    def upload_input_data(self, value):

        if not isinstance(value, list):
            raise ree.TypeError(expected_type=list, actual_type=type(value))

        self._upload_input_data = value


    @copy_input_data.setter
    def copy_input_data(self, value):

        if not isinstance(value, list):
            raise ree.TypeError(expected_type=list, actual_type=type(value))

        self._copy_input_data = value


    @move_input_data.setter
    def move_input_data(self, value):

        if not isinstance(value, list):
            raise ree.TypeError(expected_type=list, actual_type=type(value))

        self._move_input_data = value


    @link_input_data.setter
    def link_input_data(self, value):

        if not isinstance(value, list):
            raise ree.TypeError(expected_type=list, actual_type=type(value))

        self._link_input_data = value


    @copy_output_data.setter
    def copy_output_data(self, value):

        if not isinstance(value, list):
            raise ree.TypeError(expected_type=list, actual_type=type(value))

        self._copy_output_data = value


    @link_output_data.setter
    def link_output_data(self, value):

        if not isinstance(value, list):
            raise ree.TypeError(expected_type=list, actual_type=type(value))

        self._link_output_data = value


    @move_output_data.setter
    def move_output_data(self, value):

        if not isinstance(value, list):
            raise ree.TypeError(expected_type=list, actual_type=type(value))

        self._move_output_data = value


    @download_output_data.setter
    def download_output_data(self, value):

        if not isinstance(value, list):
            raise ree.TypeError(expected_type=list, actual_type=type(value))

        self._download_output_data = value


    @stdout.setter
    def stdout(self, value):

        if not isinstance(value, str):
            raise ree.TypeError(expected_type=str,
                                actual_type=type(value))

        self._stdout = value


    @stderr.setter
    def stderr(self, value):

        if not isinstance(value, str):
            raise ree.TypeError(expected_type=str,
                                actual_type=type(value))

        self._stderr = value


    @exit_code.setter
    def exit_code(self, value):

        if not isinstance(value, int):
            raise ree.TypeError(entity='exit_code', expected_type=int,
                                actual_type=type(value))

        self._exit_code = value


    @path.setter
    def path(self, value):

        if not isinstance(value, str):
            raise ree.TypeError(entity='path', expected_type=str,
                                actual_type=type(value))

        self._path = value


    @tag.setter
    def tag(self, value):

        warnings.warn("Attribute tag is depcrecated. Use tags instead", DeprecationWarning)

        # this method exists for backward compatibility
        if not isinstance(value, str):
            raise ree.TypeError(entity='tag', expected_type=str,
                                actual_type=type(value))
        self._tags = {'colocate': value}


    @tags.setter
    def tags(self, value):

        if not isinstance(value, dict):
            raise ree.TypeError(entity='tags', expected_type=dict,
                                actual_type=type(value))

        if list(value.keys()) != ['colocate']:
            raise ree.TypeError(expected_type=dict,
                                actual_type=type(value.get('colocate')),
                                entity='colocate')

        if not isinstance(value['colocate'], str):
            raise ree.TypeError(entity='tag', expected_type=str,
                                actual_type=type(value))

        self._tags = value


    @parent_stage.setter
    def parent_stage(self, value):

        if not isinstance(value, dict):
            raise ree.TypeError(expected_type=dict, actual_type=type(value))

        self._p_stage = value


    @parent_pipeline.setter
    def parent_pipeline(self, value):

        if not isinstance(value, dict):
            raise ree.TypeError(expected_type=dict, actual_type=type(value))

        self._p_pipeline = value


    @stage_on_error.setter
    def stage_on_error(self, value):

        if not isinstance(value, bool):
            raise ree.TypeError(expected_type=bool, actual_type=type(value))

        self._stage_on_error = value
    # --------------------------------------------------------------------------
    #
    def to_dict(self):
        '''
        Convert current Task into a dictionary

        :return: python dictionary
        '''

        task_desc_as_dict = {
            'uid'                  : self._uid,
            'name'                 : self._name,
            'state'                : self._state,
            'state_history'        : self._state_history,

            'pre_exec'             : self._pre_exec,
            'executable'           : self._executable,
            'arguments'            : self._arguments,
            'sandbox'              : self._sandbox,
            'post_exec'            : self._post_exec,
            'cpu_reqs'             : self.cpu_reqs,
            'gpu_reqs'             : self.gpu_reqs,
            'lfs_per_process'      : self._lfs_per_process,

            'upload_input_data'    : self._upload_input_data,
            'copy_input_data'      : self._copy_input_data,
            'link_input_data'      : self._link_input_data,
            'move_input_data'      : self._move_input_data,
            'copy_output_data'     : self._copy_output_data,
            'link_output_data'     : self._link_output_data,
            'move_output_data'     : self._move_output_data,
            'download_output_data' : self._download_output_data,

            'stdout'               : self._stdout,
            'stderr'               : self._stderr,
            'stage_on_error'       : self._stage_on_error,

            'exit_code'            : self._exit_code,
            'path'                 : self._path,
            'tags'                 : self._tags,
            'rts_uid'              : self._rts_uid,

            'parent_stage'         : self._p_stage,
            'parent_pipeline'      : self._p_pipeline,
        }

        return task_desc_as_dict


    # --------------------------------------------------------------------------
    #
    def from_dict(self, d):
        '''
        Create a Task from a dictionary. The change is in inplace.

        :argument: python dictionary
        :return: None
        '''

        invalid_symbols = punctuation.replace('.','')
        # FIXME: uid, name, state and state_history to use setter type checks
        if d.get('uid') is not None:
            if any(symbol in d['uid'] for symbol in invalid_symbols):
                raise ree.ValueError(obj=self._uid,
                                     attribute='uid',
                                     actual_value=d['uid'],
                                     expected_value=NAME_MESSAGE)
            else:
                self._uid = d['uid']

        if d.get('name') is not None:
            if any(symbol in d['name'] for symbol in invalid_symbols):
                raise ree.ValueError(obj=self._uid,
                                     attribute='name',
                                     actual_value=d['name'],
                                     expected_value=NAME_MESSAGE)
            else:
                self._name = d['name']

        if 'state' not in d:
            self._state = res.INITIAL
        else:
            # avoid adding state to state history, thus do typecheck here
            if not isinstance(d['state'], str):
                raise ree.TypeError(entity='state', expected_type=str,
                                    actual_type=type(d['state']))
            self._state = d['state']


        # for all other attributes, we use the type and value checks in the
        # class setters
        for k, v in d.items():
            if k not in ['uid', 'name', 'state']:
                if v is not None:
                    setattr(self, k, v)


    # --------------------------------------------------------------------------
    def _validate(self):
        '''
        Purpose: Validate that the state of the task is 'DESCRIBED' and that an
        executable has been specified for the task.
        '''

        if self._uid in Task._uids:
            raise ree.EnTKError(msg='Task ID %s already exists' % self._uid)
        else:
            Task._uids.append(self._uid)

        if self._state is not res.INITIAL:
            raise ree.ValueError(obj=self._uid, attribute='state',
                                 expected_value=res.INITIAL,
                                 actual_value=self._state)

        if not self._executable:
            raise ree.MissingError(obj=self._uid,
                                   missing_attribute='executable')


# ------------------------------------------------------------------------------

