import radical.utils as ru
from radical.entk.exceptions import *
from radical.entk import states


class Task(object):

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

    def __init__(self):

        self._uid = None
        self._name = None

        self._state = states.INITIAL

        # Attributes necessary for execution
        self._pre_exec = list()
        self._executable = list()
        self._arguments = list()
        self._post_exec = list()
        self._cpu_reqs = {'processes': 1,
                          'process_type': None,
                          'threads_per_process': 1,
                          'thread_type': None
                          }
        self._gpu_reqs = {'processes': 0,
                          'process_type': None,
                          'threads_per_process': 0,
                          'thread_type': None
                          }
        self._lfs_per_process = 0

        # Data staging attributes
        self._upload_input_data = list()
        self._copy_input_data = list()
        self._link_input_data = list()
        self._move_input_data = list()
        self._copy_output_data = list()
        self._move_output_data = list()
        self._download_output_data = list()

        # Name of file to write stdout and stderr of task
        self._stdout = None
        self._stderr = None

        # Additional attributes that help in mapping tasks
        # to cuds and cus to tasks
        self._path = None
        self._exit_code = None
        self._tag = None

        # Keep track of states attained
        self._state_history = [states.INITIAL]

        # The following help in updation
        # Stage this task belongs to
        self._p_stage = {'uid': None, 'name': None}
        # Pipeline this task belongs to
        self._p_pipeline = {'uid': None, 'name': None}

    # ------------------------------------------------------------------------------------------------------------------
    # Getter functions
    # ------------------------------------------------------------------------------------------------------------------

    @property
    def uid(self):
        """
        Unique ID of the current task

        :getter: Returns the unique id of the current task
        :type: String
        """
        return self._uid

    @property
    def name(self):
        """
        Name of the task. Do not use a ',' or '_' in an object's name.

        :getter: Returns the name of the current task
        :setter: Assigns the name of the current task
        :type: String
        """

        return self._name

    @property
    def state(self):
        """
        Current state of the task

        :getter: Returns the state of the current task
        :type: String
        """

        return self._state

    @property
    def pre_exec(self):
        """
        List of commands to be executed prior to the executable

        :getter: return the list of commands
        :setter: assign the list of commands
        :arguments: list of strings
        """
        return self._pre_exec

    @property
    def executable(self):
        """
        A unix-based kernel to be executed

        :getter: returns the executable of the current task
        :setter: assigns the executable for the current task
        :arguments: string
        """
        return self._executable

    @property
    def arguments(self):
        """
        List of arguments to be supplied to the executable

        :getter: returns the list of arguments of the current task
        :setter: assigns a list of arguments to the current task
        :arguments: list of strings
        """
        return self._arguments

    @property
    def post_exec(self):
        """
        List of commands to be executed post executable

        :getter: return the list of commands
        :setter: assign the list of commands
        :arguments: list of strings
        """

        return self._post_exec

    @property
    def cpu_reqs(self):
        """
        **Purpose:** The CPU requirements of the current Task.

        The requirements are described in terms of the number of processes and threads to
        be run in this Task. The expected format is:

        task.cpu_reqs = {
                            |  'processes': X,
                            |  'process_type': None/MPI,
                            |  'threads_per_process': Y,
                            |  'thread_type': None/OpenMP
                        }

        This description means that the Task is going to spawn X processes and Y threads
        per each of these processes to run on CPUs. Hence, the total number of cpus required by the
        Task is X*Y for all the processes and threads to execute concurrently. The
        same assumption is made in implementation and X*Y cpus are requested for this
        Task.

        The default value is:

        task.cpu_reqs = {
                            |  'processes': 1,
                            |  'process_type': None,
                            |  'threads_per_process': 1,
                            |  'thread_type': None
                        }

        This description requests 1 core and expected the executable to non-MPI and
        single threaded.

        :getter: return the cpu requirement of the current Task
        :setter: assign the cpu requirement of the current Task
        :arguments: dict

        """

        return self._cpu_reqs

    @property
    def gpu_reqs(self):
        """
        **Purpose:** The GPU requirements of the current Task.

        The requirements are described in terms of the number of processes and threads to
        be run in this Task. The expected format is:

        task.gpu_reqs = {
                            |  'processes': X,
                            |  'process_type': None/MPI,
                            |  'threads_per_process': Y,
                            |  'thread_type': None/OpenMP
                        }

        This description means that the Task is going to spawn X processes and Y threads
        per each of these processes to run on GPUs. Hence, the total number of gpus required by the
        Task is X*Y for all the processes and threads to execute concurrently. The
        same assumption is made in implementation and X*Y gpus are requested for this
        Task.

        The default value is:

        task.gpu_reqs = {
                            |  'processes': 0,
                            |  'process_type': None,
                            |  'threads_per_process': 0,
                            |  'thread_type': None
                        }

        This description requests 0 gpus as not all machines have GPUs.

        :getter: return the gpu requirement of the current Task
        :setter: assign the gpu requirement of the current Task
        :arguments: dict

        """

        return self._gpu_reqs

    @property
    def lfs_per_process(self):
        """
        Set the amount of local file-storage space required by the task
        """
        return self._lfs_per_process

    @property
    def upload_input_data(self):
        """
        List of files to be transferred from local machine to the location of the current task
        on the remote machine

        :getter: return the list of files
        :setter: assign the list of files
        :arguments: list of strings
        """

        return self._upload_input_data

    @property
    def copy_input_data(self):
        """
        List of files to be copied from a location on the remote machine to the location of
        current task on the remote machine

        :getter: return the list of files
        :setter: assign the list of files
        :arguments: list of strings
        """

        return self._copy_input_data

    @property
    def link_input_data(self):
        """
        List of files to be linked from a location on the remote machine to the location of
        current task on the remote machine

        :getter: return the list of files
        :setter: assign the list of files
        :arguments: list of strings
        """

        return self._link_input_data


    @property
    def move_input_data(self):
        """
        List of files to be move from a location on the remote machine to the location of
        current task on the remote machine

        :getter: return the list of files
        :setter: assign the list of files
        :arguments: list of strings
        """

        return self._move_input_data

    @property
    def copy_output_data(self):
        """
        List of files to be copied from the location of the current task to another location
        on the remote machine

        :getter: return the list of files
        :setter: assign the list of files
        :arguments: list of strings
        """

        return self._copy_output_data


    @property
    def move_output_data(self):
        """
        List of files to be copied from the location of the current task to another location
        on the remote machine

        :getter: return the list of files
        :setter: assign the list of files
        :arguments: list of strings
        """

        return self._move_output_data

    @property
    def download_output_data(self):
        """
        List of files to be downloaded from the location of the current task to a location
        on the local machine.

        :getter: return the list of files
        :setter: assign the list of files
        :arguments: list of strings
        """
        return self._download_output_data

    @property
    def stdout(self):
        """
        Name of the file to which stdout of task is to be written

        :getter: return name of stdout file
        :setter: assign name of stdout file
        :arguments: str
        """

        return self._stdout

    @property
    def stderr(self):
        """
        Name of the file to which stderr of task is to be written

        :getter: return name of stderr file
        :setter: assign name of stderr file
        :arguments: str
        """

        return self._stderr


    @property
    def exit_code(self):
        """
        Get the exit code for DONE tasks. 0 for successful tasks, 1 for failed tasks.

        :getter: return the exit code of the current task
        """
        return self._exit_code

    @property
    def path(self):
        """
        Get the path of the task on the remote machine. Useful to reference files
        generated in the current task.

        :getter: return the path of the current task
        """

        return self._path

    @property
    def tag(self):
        """
        Set the tag for the task that can be used while scheduling by the RTS

        :getter: return the tag of the current task
        """

        return self._tag

    @property
    def parent_stage(self):
        """
        :getter: Returns the stage this task belongs to
        :setter: Assigns the stage uid this task belongs to
        """
        return self._p_stage

    @property
    def parent_pipeline(self):
        """
        :getter: Returns the pipeline this task belongs to
        :setter: Assigns the pipeline uid this task belongs to
        """

        return self._p_pipeline

    @property
    def state_history(self):
        """
        Returns a list of the states obtained in temporal order

        :return: list
        """

        return self._state_history

    # ------------------------------------------------------------------------------------------------------------------
    # Setter functions
    # ------------------------------------------------------------------------------------------------------------------

    @uid.setter
    def uid(self, val):
        if isinstance(val, str):
            self._uid = val
        else:
            raise TypeError(expected_type=str, actual_type=type(val))

    @name.setter
    def name(self, value):
        if isinstance(value, str):
            if ',' in value:
                raise Error(
                    "Using ',' or '_' in an object's name may corrupt the profiling and internal mapping tables")
            else:
                self._name = value
        else:
            raise TypeError(expected_type=str, actual_type=type(value))

    @state.setter
    def state(self, value):
        if isinstance(value, str):
            if value in states._task_state_values.keys():
                self._state = value
                self._state_history.append(value)
            else:
                raise ValueError(obj=self._uid,
                                 attribute='state',
                                 expected_value=states._task_state_values.keys(),
                                 actual_value=value)
        else:
            raise TypeError(expected_type=str, actual_type=type(val))

    @pre_exec.setter
    def pre_exec(self, val):
        if isinstance(val, list):
            self._pre_exec = val
        else:
            raise TypeError(expected_type=list, actual_type=type(val))

    @executable.setter
    def executable(self, val):
        if isinstance(val, list):
            self._executable = val
        elif isinstance(val, str):
            self._executable = [val]
        else:
            raise TypeError(expected_type='list or str', actual_type=type(val))

    @arguments.setter
    def arguments(self, val):
        if isinstance(val, list):
            self._arguments = val
        else:
            raise TypeError(expected_type=list, actual_type=type(val))

    @post_exec.setter
    def post_exec(self, val):
        if isinstance(val, list):
            self._post_exec = val
        else:
            raise TypeError(expected_type=list, actual_type=type(val))

    @cpu_reqs.setter
    def cpu_reqs(self, val):
        if isinstance(val, dict):

            expected_keys = set(
                ['processes', 'threads_per_process', 'process_type', 'thread_type'])

            if set(val.keys()) <= expected_keys:

                if type(val.get('processes')) in [type(None), int]:
                    self._cpu_reqs['processes'] = val.get('processes', 1)
                else:
                    raise TypeError(expected_type=int,
                                    actual_type=type(val.get('processes')),
                                    entity='processes'
                                    )

                if val.get('process_type') in [None, 'MPI', '']:
                    self._cpu_reqs['process_type'] = val.get('process_type')
                else:
                    raise ValueError(expected_value='None or MPI',
                                     actual_value=val.get('process_type'),
                                     obj='cpu_reqs',
                                     attribute='process_type'
                                     )

                if type(val.get('threads_per_process')) in [type(None), int]:
                    self._cpu_reqs['threads_per_process'] = val.get(
                        'threads_per_process', 1)
                else:
                    raise TypeError(expected_type=int,
                                    actual_type=type(
                                        val.get('threads_per_process')),
                                    entity='threads_per_process'
                                    )

                if val.get('thread_type') in [None, 'OpenMP', '']:
                    self._cpu_reqs['thread_type'] = val.get('thread_type')
                else:
                    raise ValueError(expected_value='None or OpenMP',
                                     actual_value=val.get('thread_type'),
                                     obj='cpu_reqs',
                                     attribute='thread_type'
                                     )

            else:
                raise MissingError(
                    obj='cpu_reqs', missing_attribute=expected_keys - set(val.keys()))

    @gpu_reqs.setter
    def gpu_reqs(self, val):
        if isinstance(val, dict):

            expected_keys = set(
                ['processes', 'threads_per_process', 'process_type', 'thread_type'])

            if set(val.keys()) <= expected_keys:

                if type(val.get('processes')) in [type(None), int]:
                    self._gpu_reqs['processes'] = val.get('processes', 1)
                else:
                    raise TypeError(expected_type=dict,
                                    actual_type=type(val.get('processes')),
                                    entity='processes'
                                    )

                if val.get('process_type') in [None, 'MPI', '']:
                    self._gpu_reqs['process_type'] = val.get('process_type')
                else:
                    raise ValueError(expected_value='None or MPI',
                                     actual_value=val.get('process_type'),
                                     obj='gpu_reqs',
                                     attribute='process_type'
                                     )

                if type(val.get('threads_per_process')) in [type(None), int]:
                    self._gpu_reqs['threads_per_process'] = val.get(
                        'threads_per_process', 1)
                else:
                    raise TypeError(expected_type=int,
                                    actual_type=type(
                                        val.get('threads_per_process')),
                                    entity='threads_per_process'
                                    )

                if val.get('thread_type') in [None, 'OpenMP', '']:
                    self._gpu_reqs['thread_type'] = val.get('thread_type')
                else:
                    raise ValueError(expected_value='None or OpenMP',
                                     actual_value=val.get('thread_type'),
                                     obj='gpu_reqs',
                                     attribute='thread_type'
                                     )

            else:
                raise MissingError(
                    obj='gpu_reqs', missing_attribute=expected_keys - set(val.keys()))

    @lfs_per_process.setter
    def lfs_per_process(self, val):
        if isinstance(val, int):
            self._lfs_per_process = val
        else:
            raise TypeError(expected_type=int, actual_type=type(val))

    @upload_input_data.setter
    def upload_input_data(self, val):
        if isinstance(val, list):
            self._upload_input_data = val
        else:
            raise TypeError(expected_type=list, actual_type=type(val))

    @copy_input_data.setter
    def copy_input_data(self, val):
        if isinstance(val, list):
            self._copy_input_data = val
        else:
            raise TypeError(expected_type=list, actual_type=type(val))


    @move_input_data.setter
    def move_input_data(self, val):
        if isinstance(val, list):
            self._move_input_data = val
        else:
            raise TypeError(expected_type=list, actual_type=type(val))

    @link_input_data.setter
    def link_input_data(self, val):
        if isinstance(val, list):
            self._link_input_data = val
        else:
            raise TypeError(expected_type=list, actual_type=type(val))

    @copy_output_data.setter
    def copy_output_data(self, val):
        if isinstance(val, list):
            self._copy_output_data = val
        else:
            raise TypeError(expected_type=list, actual_type=type(val))


    @move_output_data.setter
    def move_output_data(self, val):
        if isinstance(val, list):
            self._move_output_data = val
        else:
            raise TypeError(expected_type=list, actual_type=type(val))

    @download_output_data.setter
    def download_output_data(self, val):
        if isinstance(val, list):
            self._download_output_data = val
        else:
            raise TypeError(expected_type=list, actual_type=type(val))

    @stdout.setter
    def stdout(self, val):
        if isinstance(val, str):
            self._stdout = val
        else:
            raise TypeError(expected_type=str, actual_type=type(val))

    @stderr.setter
    def stderr(self, val):
        if isinstance(val, str):
            self._stderr = val
        else:
            raise TypeError(expected_type=str, actual_type=type(val))

    @exit_code.setter
    def exit_code(self, val):
        if isinstance(val, int):
            self._exit_code = val
        else:
            raise TypeError(entity='exit_code',
                            expected_type=int, actual_type=type(val))

    @path.setter
    def path(self, val):
        if isinstance(val, str):
            self._path = val
        else:
            raise TypeError(entity='path', expected_type=str,
                            actual_type=type(val))

    @tag.setter
    def tag(self, val):
        if isinstance(val, str):
            self._tag = val
        else:
            raise TypeError(entity='tag', expected_type=str,
                            actual_type=type(val))

    @parent_stage.setter
    def parent_stage(self, val):
        if isinstance(val, dict):
            self._p_stage = val
        else:
            raise TypeError(expected_type=dict, actual_type=type(val))

    @parent_pipeline.setter
    def parent_pipeline(self, val):
        if isinstance(val, dict):
            self._p_pipeline = val
        else:
            raise TypeError(expected_type=dict, actual_type=type(val))

    # ------------------------------------------------------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------------------------------------------------------

    def to_dict(self):
        """
        Convert current Task into a dictionary

        :return: python dictionary
        """

        task_desc_as_dict = {
            'uid': self._uid,
            'name': self._name,
            'state': self._state,
            'state_history': self._state_history,

            'pre_exec': self._pre_exec,
            'executable': self._executable,
            'arguments': self._arguments,
            'post_exec': self._post_exec,
            'cpu_reqs': self._cpu_reqs,
            'gpu_reqs': self._gpu_reqs,
            'lfs_per_process': self._lfs_per_process,

            'upload_input_data': self._upload_input_data,
            'copy_input_data': self._copy_input_data,
            'link_input_data': self._link_input_data,
            'move_input_data': self._move_input_data,
            'copy_output_data': self._copy_output_data,
            'move_output_data': self._move_output_data,
            'download_output_data': self._download_output_data,

            'stdout': self._stdout,
            'stderr': self._stderr,

            'exit_code': self._exit_code,
            'path': self._path,
            'tag': self._tag,

            'parent_stage': self._p_stage,
            'parent_pipeline': self._p_pipeline,
        }

        return task_desc_as_dict

    def from_dict(self, d):
        """
        Create a Task from a dictionary. The change is in inplace.

        :argument: python dictionary
        :return: None
        """

        if 'uid' in d:
            if d['uid']:
                self._uid = d['uid']

        if 'name' in d:
            if d['name']:
                self._name = d['name']

        if 'state' in d:
            if isinstance(d['state'], str) or isinstance(d['state'], unicode):
                self._state = d['state']
            else:
                raise TypeError(entity='state', expected_type=str,
                                actual_type=type(d['state']))
        else:
            self._state = states.INITIAL

        if 'state_history' in d:
            if isinstance(d['state_history'], list):
                self._state_history = d['state_history']
            else:
                raise TypeError(entity='state_history', expected_type=list, actual_type=type(
                    d['state_history']))

        if 'pre_exec' in d:
            if isinstance(d['pre_exec'], list):
                self._pre_exec = d['pre_exec']
            else:
                raise TypeError(expected_type=list,
                                actual_type=type(d['pre_exec']))

        if 'executable' in d:
            if isinstance(d['executable'], list):
                self._executable = d['executable']
            else:
                raise TypeError(expected_type=list,
                                actual_type=type(d['executable']))

        if 'arguments' in d:
            if isinstance(d['arguments'], list):
                self._arguments = d['arguments']
            else:
                raise TypeError(expected_type=list,
                                actual_type=type(d['arguments']))

        if 'post_exec' in d:
            if isinstance(d['post_exec'], list):
                self._post_exec = d['post_exec']
            else:
                raise TypeError(expected_type=list,
                                actual_type=type(d['post_exec']))

        if 'cpu_reqs' in d:
            if isinstance(d['cpu_reqs'], dict):
                self._cpu_reqs = d['cpu_reqs']
            else:
                raise TypeError(expected_type=dict,
                                actual_type=type(d['cpu_reqs']))

        if 'gpu_reqs' in d:
            if isinstance(d['gpu_reqs'], dict):
                self._gpu_reqs = d['gpu_reqs']
            else:
                raise TypeError(expected_type=dict,
                                actual_type=type(d['gpu_reqs']))

        if 'lfs_per_process' in d:
            if d['lfs_per_process']:
                if isinstance(d['lfs_per_process'], int):
                    self._lfs_per_process = d['lfs_per_process']
                else:
                    raise TypeError(expected_type=int,
                                    actual_type=type(d['lfs_per_process']))

        if 'upload_input_data' in d:
            if isinstance(d['upload_input_data'], list):
                self._upload_input_data = d['upload_input_data']
            else:
                raise TypeError(expected_type=list,
                                actual_type=type(d['upload_input_data']))

        if 'copy_input_data' in d:
            if isinstance(d['copy_input_data'], list):
                self._copy_input_data = d['copy_input_data']
            else:
                raise TypeError(expected_type=list,
                                actual_type=type(d['copy_input_data']))

        if 'link_input_data' in d:
            if isinstance(d['link_input_data'], list):
                self._link_input_data = d['link_input_data']
            else:
                raise TypeError(expected_type=list,
                                actual_type=type(d['link_input_data']))

        if 'move_input_data' in d:
            if isinstance(d['move_input_data'], list):
                self._move_input_data = d['move_input_data']
            else:
                raise TypeError(expected_type=list,
                                actual_type=type(d['move_input_data']))


        if 'copy_output_data' in d:
            if isinstance(d['copy_output_data'], list):
                self._copy_output_data = d['copy_output_data']
            else:
                raise TypeError(expected_type=list,
                                actual_type=type(d['copy_output_data']))

        if 'move_output_data' in d:
            if isinstance(d['move_output_data'], list):
                self._move_output_data = d['move_output_data']
            else:
                raise TypeError(expected_type=list,
                                actual_type=type(d['move_output_data']))

        if 'download_output_data' in d:
            if isinstance(d['download_output_data'], list):
                self._download_output_data = d['download_output_data']
            else:
                raise TypeError(expected_type=list, actual_type=type(
                    d['download_output_data']))

        if 'stdout' in d:
            if d['stdout']:
                if isinstance(d['stdout'], str) or isinstance(d['stdout'], unicode):
                    self._stdout = d['stdout']
                else:
                    raise TypeError(expected_type=str, actual_type=type(d['stdout']))

        if 'stderr' in d:
            if d['stderr']:
                if isinstance(d['stderr'], str) or isinstance(d['stderr'], unicode):
                    self._stderr = d['stderr']
                else:
                    raise TypeError(expected_type=str, actual_type=type(d['stderr']))

        if 'exit_code' in d:
            if d['exit_code']:
                if isinstance(d['exit_code'], int):
                    self._exit_code = d['exit_code']
                else:
                    raise TypeError(
                        entity='exit_code', expected_type=int, actual_type=type(d['exit_code']))

        if 'path' in d:
            if d['path']:
                if isinstance(d['path'], str) or isinstance(d['path'], unicode):
                    self._path = d['path']
                else:
                    raise TypeError(entity='path', expected_type=str,
                                    actual_type=type(d['path']))

        if 'tag' in d:
            if d['tag']:
                if isinstance(d['tag'], str) or isinstance(d['tag'], unicode):
                    self._tag = str(d['tag'])
                else:
                    raise TypeError(expected_type=str,
                                    actual_type=type(d['tag']))

        if 'parent_stage' in d:
            if isinstance(d['parent_stage'], dict):
                self._p_stage = d['parent_stage']
            else:
                raise TypeError(
                    entity='parent_stage', expected_type=dict, actual_type=type(d['parent_stage']))

        if 'parent_pipeline' in d:
            if isinstance(d['parent_pipeline'], dict):
                self._p_pipeline = d['parent_pipeline']
            else:
                raise TypeError(entity='parent_pipeline', expected_type=dict, actual_type=type(
                    d['parent_pipeline']))

    # ------------------------------------------------------------------------------------------------------------------
    # Private methods
    # ------------------------------------------------------------------------------------------------------------------

    def _assign_uid(self, sid):
        """
        Purpose: Assign a uid to the current object based on the sid passed
        """
        self._uid = ru.generate_id(
            'task.%(item_counter)04d', ru.ID_CUSTOM, namespace=sid)

    def _validate(self):
        """
        Purpose: Validate that the state of the task is 'DESCRIBED' and that an executable has been specified for the
        task.
        """

        if self._state is not states.INITIAL:
            raise ValueError(obj=self._uid,
                             attribute='state',
                             expected_value=states.INITIAL,
                             actual_value=self._state)

        if not self._executable:
            raise MissingError(obj=self._uid,
                               missing_attribute='executable')
    # ------------------------------------------------------------------------------------------------------------------
