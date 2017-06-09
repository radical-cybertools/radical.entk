import radical.utils as ru
from radical.entk.exceptions import *
from radical.entk import states

class Task(object):


    """
    A Task is an abstraction of a computational unit. In this case, a Task consists of its
    executable along with its required software environment, files to be staged as input 
    and output.
    """

    def __init__(self):

        self._uid       = ru.generate_id('radical.entk.task')
        self._name      = str()

        self._state     = states.UNSCHEDULED

        # Attributes necessary for execution
        self._pre_exec      = list()
        self._executable    = list()
        self._arguments     = list()
        self._post_exec     = list()
        self._cores  = 1
        self._mpi = False

        # Data staging attributes
        self._upload_input_data     = list()
        self._copy_input_data       = list()
        self._link_input_data       = list()
        self._copy_output_data      = list()
        self._download_output_data  = list()


        ## The following help in updation
        # Stage this task belongs to
        self._p_stage = None
        # Pipeline this task belongs to
        self._p_pipeline = None


    # -----------------------------------------------
    # Getter functions
    # -----------------------------------------------

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
        Name of the task

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
    def cores(self):

        """
        Number of cores to be used for the current task

        :getter: return the number of cores
        :setter: assign the number of cores
        :arguments: integer
        """

        return self._cores

    @property
    def mpi(self):

        """
        Flag to enable MPI-based execution

        :getter: return the mpi flag
        :setter: assign the mpi flag
        """

        return self._mpi

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
    def _parent_stage(self):

        """
        :getter: Returns the stage this task belongs to
        :setter: Assigns the stage uid this task belongs to
        """
        return self._p_stage
    
    @property
    def _parent_pipeline(self):

        """
        :getter: Returns the pipeline this task belongs to
        :setter: Assigns the pipeline uid this task belongs to
        """

        return self._p_pipeline    
    # -----------------------------------------------


    # -----------------------------------------------
    # Setter functions
    # -----------------------------------------------

    @uid.setter
    def uid(self, value):
        if isinstance(value, str):
            self._uid = value
        else:
            raise TypeError(expected_type=str, actual_type=type(value))

    @name.setter
    def name(self, value):
        if isinstance(value,str):
            self._name = value
        else:
            raise TypeError(expected_type=str, actual_type=type(value))

    @state.setter
    def state(self, value):
        if isinstance(value,str):
            self._state = value
        else:
            raise TypeError(expected_type=str, actual_type=type(value))

    @pre_exec.setter
    def pre_exec(self, value):
        if isinstance(value, list):
            self._pre_exec = value
        else:
            raise TypeError(expected_type=list, actual_type=type(value))


    @executable.setter
    def executable(self, value):
        if isinstance(value, list):
            self._executable = value
        else:
            raise TypeError(expected_type=list, actual_type=type(value))

    @arguments.setter
    def arguments(self, value):
        if isinstance(value, list):
            self._arguments = value
        else:
            raise TypeError(expected_type=list, actual_type=type(value))


    @post_exec.setter
    def post_exec(self, value):
        if isinstance(value, list):
            self._post_exec = value
        else:
            raise TypeError(expected_type=list, actual_type=type(value))

    @cores.setter
    def cores(self, val):
        if isinstance(val, int):
            self._cores = val
        else:
            raise TypeError(expected_type=int, actual_type=type(val))

    @mpi.setter
    def mpi(self, value):
        if isinstance(val, bool):
            self._mpi = val
        else:
            raise TypeError(expected_type=bool, actual_type=type(val))


    @upload_input_data.setter
    def upload_input_data(self, value):
        if isinstance(value, list):
            self._upload_input_data = value
        else:
            raise TypeError(expected_type=list, actual_type=type(value))

    @copy_input_data.setter
    def copy_input_data(self, value):
        if isinstance(value, list):
            self._copy_input_data = value
        else:
            raise TypeError(expected_type=list, actual_type=type(value))

    @link_input_data.setter
    def link_input_data(self, value):
        if isinstance(value, list):
            self._link_input_data = value
        else:
            raise TypeError(expected_type=list, actual_type=type(value))

    @copy_output_data.setter
    def copy_output_data(self, value):
        if isinstance(value, list):
            self._copy_output_data = value
        else:
            raise TypeError(expected_type=list, actual_type=type(value))

    @download_output_data.setter
    def download_output_data(self, value):
        if isinstance(value, list):
            self._download_output_data = value
        else:
            raise TypeError(expected_type=list, actual_type=type(value))

    @_parent_stage.setter
    def _parent_stage(self, value):
        if isinstance(value,str):
            self._p_stage = value
        else:
            raise TypeError(expected_type=str, actual_type=type(value))

    @_parent_pipeline.setter
    def _parent_pipeline(self, value):
        if isinstance(value,str):
            self._p_pipeline = value
        else:
            raise TypeError(expected_type=str, actual_type=type(value))
    # -----------------------------------------------
        
    def _replicate(self, original_task):

        """
        Replicate an existing task with a new uid and UNSCHEDULED state. The change is in inplace.

        :return: None
        """

        self._uid       = ru.generate_id('radical.entk.task')
        self._name      = original_task.name

        self._state     = states.UNSCHEDULED

        # Attributes necessary for execution
        self._pre_exec      = original_task.pre_exec
        self._executable    = original_task.executable
        self._arguments     = original_task.arguments
        self._post_exec     = original_task.post_exec

        # Data staging attributes
        self._upload_input_data     = original_task.upload_input_data
        self._copy_input_data       = original_task.copy_input_data
        self._link_input_data       = original_task.link_input_data
        self._copy_output_data      = original_task.copy_output_data
        self._download_output_data  = original_task.download_output_data


        ## The following help in updation
        # Stage this task belongs to
        self._p_stage = original_task._parent_stage
        # Pipeline this task belongs to
        self._p_pipeline = original_task._parent_pipeline



    def to_dict(self):

        """
        Convert current Task into a dictionary

        :return: python dictionary
        """

        task_desc_as_dict = {
                        'uid': self._uid,
                        'name': self._name,
                        'state': self._state,

                        'pre_exec': self._pre_exec,
                        'executable': self._executable,
                        'arguments': self._arguments,
                        'post_exec': self._post_exec,
                        'cores': self._cores,

                        'upload_input_data': self._upload_input_data,
                        'copy_input_data': self._copy_input_data,
                        'link_input_data': self._link_input_data,
                        'copy_output_data': self._copy_output_data,
                        'download_output_data': self._download_output_data,

                        'parent_stage': self._p_stage,
                        'parent_pipeline': self._p_pipeline,
                    }

        return task_desc_as_dict
        
        

    def load_from_dict(self, d):

        """
        Create a Task from a dictionary. The change is in inplace.

        :argument: python dictionary
        :return: None
        """

        if 'uid' in d:
            self._uid   = d['uid']
        
        if 'name' in d:
            self._name = d['name']

        if 'state' in d:
            self._state = d['state']

        if 'pre_exec' in d:
            self._pre_exec = d['pre_exec']

        if 'executable' in d:
            self._executable = d['executable']

        if 'arguments' in d:
            self._arguments = d['arguments']

        if 'post_exec' in d:
            self._post_exec = d['post_exec']

        if 'cores' in d:
            self._cores = d['cores']
            
        if 'upload_input_data' in d:                                    
            self._upload_input_data = d['upload_input_data']

        if 'copy_input_data' in d:
            self._copy_input_data = d['copy_input_data']

        if 'link_input_data' in d:
            self._link_input_data = d['link_input_data']

        if 'copy_output_data' in d:
            self._copy_output_data = d['copy_output_data']

        if 'download_output_data' in d:
            self._download_output_data = d['download_output_data']                                                

        if 'parent_stage' in d:
            self._p_stage = d['parent_stage']

        if 'parent_pipeline' in d:
            self._p_pipeline = d['parent_pipeline']
                                                
