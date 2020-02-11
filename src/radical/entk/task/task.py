
import radical.utils as ru

from .. import exceptions as ree
from .. import states     as res


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

    # FIXME: this should be converted into an RU/RS Attribute object, almost all
    #        of the code is redundant with the attribute class...

    # --------------------------------------------------------------------------
    #
    def __init__(self, from_dict=None):

        self._uid        = None
        self._name       = None
        self._state      = res.INITIAL

        # Attributes necessary for execution
        self._pre_exec   = list()
        self._executable = None
        self._arguments  = list()
        self._sandbox    = None
        self._post_exec  = list()

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
        self._stdout = None
        self._stderr = None

        # Additional attributes that help in mapping tasks
        # to cuds and cus to tasks
        self._path      = None
        self._exit_code = None
        self._tag       = None

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

        task.cpu_reqs = {
                          | 'processes'           : X,
                          | 'process_type'        : None/MPI,
                          | 'threads_per_process' : Y,
                          | 'thread_type'         : None/OpenMP}

        This description means that the Task is going to spawn X processes and
        Y threads per each of these processes to run on CPUs. Hence, the total
        number of cpus required by the Task is X*Y for all the processes and
        threads to execute concurrently. The same assumption is made in
        implementation and X*Y cpus are requested for this Task.

        The default value is:

        task.cpu_reqs = {
                          | 'processes'           : 1,
                          | 'process_type'        : None,
                          | 'threads_per_process' : 1,
                          | 'thread_type'         : None}

        This description requests 1 core and expected the executable to non-MPI
        and single threaded.

        :getter: return the cpu requirement of the current Task
        :setter: assign the cpu requirement of the current Task
        :arguments: dict
        '''

        return self._cpu_reqs


    @property
    def gpu_reqs(self):
        '''
        **Purpose:** The GPU requirements of the current Task.

        The requirements are described in terms of the number of processes and
        threads to be run in this Task. The expected format is:

        task.gpu_reqs = {
                          | 'processes'           : X,
                          | 'process_type'        : None/MPI,
                          | 'threads_per_process' : Y,
                          | 'thread_type'         : None/OpenMP/CUDA}

        This description means that the Task is going to spawn X processes and
        Y threads per each of these processes to run on GPUs. Hence, the total
        number of gpus required by the Task is X*Y for all the processes and
        threads to execute concurrently. The same assumption is made in
        implementation and X*Y gpus are requested for this Task.

        The default value is:

        task.gpu_reqs = {
                          | 'processes'           : 0,
                          | 'process_type'        : None,
                          | 'threads_per_process' : 0,
                          | 'thread_type'         : None}

        This description requests 0 gpus as not all machines have GPUs.

        :getter: return the gpu requirement of the current Task
        :setter: assign the gpu requirement of the current Task
        :arguments: dict
        '''

        return self._gpu_reqs


    @property
    def lfs_per_process(self):
        '''
        Set the amount of local file-storage space required by the task
        '''

        return self._lfs_per_process


    @property
    def upload_input_data(self):
        '''
        List of files to be transferred from local machine to the location of
        the current task on the remote machine.

        :getter: return the list of files
        :setter: assign the list of files
        :arguments: list of strings
        '''

        return self._upload_input_data


    @property
    def copy_input_data(self):
        '''
        List of files to be copied from a location on the remote machine to the
        location of current task on the remote machine.

        :getter: return the list of files
        :setter: assign the list of files
        :arguments: list of strings
        '''

        return self._copy_input_data


    @property
    def link_input_data(self):
        '''
        List of files to be linked from a location on the remote machine to the
        location of current task on the remote machine.

        :getter: return the list of files
        :setter: assign the list of files
        :arguments: list of strings
        '''

        return self._link_input_data


    @property
    def move_input_data(self):
        '''
        List of files to be move from a location on the remote machine to the
        location of current task on the remote machine.

        :getter: return the list of files
        :setter: assign the list of files
        :arguments: list of strings
        '''

        return self._move_input_data


    @property
    def copy_output_data(self):
        '''
        List of files to be copied from the location of the current task to
        another location on the remote machine.

        :getter: return the list of files
        :setter: assign the list of files
        :arguments: list of strings
        '''

        return self._copy_output_data


    @property
    def link_output_data(self):
        '''
        List of files to be linked from the location of current task on the
        remote machine to a location on the remote machine.

        :getter: return the list of files
        :setter: assign the list of files
        :arguments: list of strings
        '''

        return self._link_output_data


    @property
    def move_output_data(self):
        '''
        List of files to be copied from the location of the current task to
        another location on the remote machine.

        :getter: return the list of files
        :setter: assign the list of files
        :arguments: list of strings
        '''

        return self._move_output_data


    @property
    def download_output_data(self):
        '''
        List of files to be downloaded from the location of the current task to
        a location on the local machine.

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
        Set the tag for the task that can be used while scheduling by the RTS

        :getter: return the tag of the current task
        '''

        return self._tag


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


    # --------------------------------------------------------------------------
    #
    @uid.setter
    def uid(self, value):

        if not isinstance(value, str):
            raise ree.TypeError(expected_type=str,
                                actual_type=type(value))

        self._uid = value


    @name.setter
    def name(self, value):

        if not isinstance(value, str):
            raise ree.TypeError(expected_type=str,
                                actual_type=type(value))

        if ',' in value:
            raise ree.ValueError(obj=self._uid,
                                 attribute='name',
                                 actual_value=value,
                                 expected_value="Using ',' in an object's name"
                                 "will corrupt internal mapping tables")

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

        if isinstance(value, list):
            value = value[0]

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

        expected_keys = set(['processes',    'threads_per_process',
                             'process_type', 'thread_type'])

        missing = expected_keys - set(value.keys())

        if missing:
            raise ree.MissingError(obj='cpu_reqs', missing_attribute=missing)

        if not isinstance(value.get('processes'), (type(None), int)):
            raise ree.TypeError(expected_type=int,
                                actual_type=type(value.get('processes')),
                                entity='processes')

        if value.get('process_type') not in [None, 'MPI', '']:
            raise ree.ValueError(expected_value='None or MPI',
                                 actual_value=value.get('process_type'),
                                 obj='cpu_reqs',
                                 attribute='process_type')

        if not isinstance(value.get('threads_per_process'), (type(None), int)):
            raise ree.TypeError(expected_type=int,
                             actual_type=type(value.get('threads_per_process')),
                             entity='threads_per_process')

        if value.get('thread_type') not in [None, 'OpenMP', '']:
            raise ree.ValueError(expected_value='None or OpenMP', obj='cpu_reqs',
                                 actual_value=value.get('thread_type'),
                                 attribute='thread_type')

        self._cpu_reqs['processes']           = value.get('processes', 1)
        self._cpu_reqs['process_type']        = value.get('process_type')
        self._cpu_reqs['threads_per_process'] = value.get('threads_per_process', 1)
        self._cpu_reqs['thread_type']         = value.get('thread_type')


    @gpu_reqs.setter
    def gpu_reqs(self, value):

        if not isinstance(value, dict):
            raise ree.TypeError(expected_type=dict, actual_type=type(value))

        expected_keys = set(['processes',    'threads_per_process',
                             'process_type', 'thread_type'])

        missing = expected_keys - set(value.keys())

        if missing:
            raise ree.MissingError(obj='gpu_reqs', missing_attribute=missing)

        if not isinstance(value.get('processes'), (type(None), int)):
            raise ree.TypeError(expected_type=dict,
                                actual_type=type(value.get('processes')),
                                entity='processes')

        if value.get('process_type') not in [None, 'MPI', '']:
            raise ree.ValueError(expected_value='None or MPI',
                                 actual_value=value.get('process_type'),
                                 obj='gpu_reqs',
                                 attribute='process_type')

        if not isinstance(value.get('threads_per_process'), (type(None), int)):
            raise ree.TypeError(expected_type=int,
                             actual_type=type(value.get('threads_per_process')),
                             entity='threads_per_process')

        if value.get('thread_type') not in [None, 'OpenMP', 'CUDA','']:
            raise ree.ValueError(expected_value='None or OpenMP or CUDA',
                                 actual_value=value.get('thread_type'),
                                 obj='gpu_reqs',
                                 attribute='thread_type')

        self._gpu_reqs['processes']           = value.get('processes', 1)
        self._gpu_reqs['process_type']        = value.get('process_type')
        self._gpu_reqs['threads_per_process'] = value.get('threads_per_process', 1)
        self._gpu_reqs['thread_type']         = value.get('thread_type')


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

        if not isinstance(value, str):
            raise ree.TypeError(entity='tag', expected_type=str,
                                actual_type=type(value))

        self._tag = value


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
            'cpu_reqs'             : self._cpu_reqs,
            'gpu_reqs'             : self._gpu_reqs,
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

            'exit_code'            : self._exit_code,
            'path'                 : self._path,
            'tag'                  : self._tag,

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

        # FIXME: uid, name, state and state_history to use setter type checks
        if d.get('uid')  is not None: self._uid  = d['uid']
        if d.get('name') is not None: self._name = d['name']

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
    #
    def _assign_uid(self, sid):
        '''
        Purpose: Assign uid to the current object based on the session ID passed
        '''

        self.uid = ru.generate_id('task.%(item_counter)04d',
                                  ru.ID_CUSTOM, ns=sid)


    # --------------------------------------------------------------------------
    def _validate(self):
        '''
        Purpose: Validate that the state of the task is 'DESCRIBED' and that an
        executable has been specified for the task.
        '''

        if self._state is not res.INITIAL:
            raise ree.ValueError(obj=self._uid, attribute='state',
                                 expected_value=res.INITIAL,
                                 actual_value=self._state)

        if not self._executable:
            raise ree.MissingError(obj=self._uid,
                                   missing_attribute='executable')


# ------------------------------------------------------------------------------

