import radical.utils as ru
from radical.entk.exceptions import *
from radical.entk.task.task import Task
from radical.entk import states
from collections import Iterable

class Stage(object):

    """
    A stage represents a collection of objects that have no relative order of execution. In this case, a
    stage consists of a set of 'Task' objects. All tasks of the same stage may execute concurrently.
    """

    def __init__(self, duplicate = False):

        if not duplicate:
            self._uid       = ru.generate_id('radical.entk.stage')
            
        self._tasks     = set()
        self._name      = str()

        self._state     = states.INITIAL

        # Keep track of states attained
        self._state_history = [states.INITIAL]

        # To change states
        self._task_count = len(self._tasks)

        # Pipeline this stage belongs to
        self._p_pipeline = None    

    # ------------------------------------------------------------------------------------------------------------------
    # Getter functions
    # ------------------------------------------------------------------------------------------------------------------

    @property
    def name(self):

        """
        Name of the stage 

        :getter: Returns the name of the current stage
        :setter: Assigns the name of the current stage
        :type: String
        """
        return self._name
    
    @property
    def tasks(self):

        """
        Tasks of the stage

        :getter: Returns all the tasks of the current stage
        :setter: Assigns tasks to the current stage
        :type: set of Tasks
        """
        return self._tasks

    @property
    def state(self):

        """
        Current state of the stage

        :getter: Returns the state of the current stage
        :type: String
        """

        return self._state

    @property
    def parent_pipeline(self):

        """
        :getter: Returns the pipeline this stage belongs to
        :setter: Assigns the pipeline uid this stage belongs to
        """
        return self._p_pipeline

    @property
    def uid(self):

        """
        Unique ID of the current stage

        :getter: Returns the unique id of the current stage
        :type: String
        """

        return self._uid

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

    @name.setter
    def name(self, value):
        if isinstance(value,str):
            self._name = value
        else:
            raise TypeError(expected_type=str, actual_type=type(value))
        

    @tasks.setter
    def tasks(self, val):        
        self._tasks = self._validate_tasks(val)
        self._task_count = len(self._tasks)
    
    @parent_pipeline.setter
    def parent_pipeline(self, value):
        if isinstance(value, str):
            self._p_pipeline = value
        else:
            raise TypeError(expected_type=str, actual_type=type(value))

    @state.setter
    def state(self, value):
        if isinstance(value,str):
            self._state = value
            self._state_history.append(value)
        else:
            raise TypeError(expected_type=str, actual_type=type(value))        
    

    # ------------------------------------------------------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------------------------------------------------------

    def add_tasks(self, val):

        """
        Adds tasks to the existing set of tasks of the Stage

        :argument: set of tasks
        """
        tasks =  self._validate_tasks(val)
        self._tasks.update(tasks)
        self._task_count = len(self._tasks)


    def to_dict(self):

        """
        Convert current Stage into a dictionary

        :return: python dictionary
        """

        stage_desc_as_dict = {

                                'uid': self._uid,
                                'name': self._name,
                                'state': self._state,
                                'state_history': self._state_history,
                                'parent_pipeline': self._p_pipeline
                        }

        return stage_desc_as_dict


    def from_dict(self, d):

        """
        Create a Stage from a dictionary. The change is in inplace.

        :argument: python dictionary
        :return: None
        """


        if 'uid' in d:
            if isinstance(d['uid'], str) or isinstance(d['uid'], unicode):
                self._uid   = d['uid']
            else:
                raise TypeError(entity='uid', expected_type=str, actual_type=type(d['uid']))

        if 'name' in d:
            if isinstance(d['name'], str) or isinstance(d['name'], unicode):
                self._name = d['name']
            else:
                raise TypeError(entity='name', expected_type=str, actual_type=type(d['name']))

        if 'state' in d:
            if isinstance(d['state'], str) or isinstance(d['state'], unicode):
                self._state = d['state']
            else:
                raise TypeError(entity='state', expected_type=str, actual_type=type(d['state']))

        else:
            self._state = states.INITIAL


        if 'state_history' in d:
            if isinstance(d['state_history'], list):
                self._state_history = d['state_history']
            else:
                raise TypeError(entity='state_history', expected_type=list, actual_type=type(d['state_history']))

        if 'parent_pipeline' in d:
            if isinstance(d['parent_pipeline'], str) or isinstance(d['parent_pipeline'], unicode):
                self._p_pipeline = d['parent_pipeline']
            else:
                raise TypeError(entity='parent_pipeline', expected_type=str, actual_type=type(d['parent_pipeline']))


    # ------------------------------------------------------------------------------------------------------------------
    # Private methods
    # ------------------------------------------------------------------------------------------------------------------

    def _pass_uid(self, tasks=None):

        """
        Purpose: Assign the parent Stage and the parent Pipeline to all the tasks of the current stage. 

        Details: This lets us trace the Stage and Pipeline if only the Task is provided.

        :arguments: set of Tasks (optional)
        :return: list of updated Tasks
        """

        if tasks is None:
            for task in self._tasks:
                task.parent_stage = self._uid
                task.parent_pipeline = self._p_pipeline
        else:
            for task in tasks:
                task.parent_stage = self._uid
                task.parent_pipeline = self._p_pipeline


            return tasks

    def _set_tasks_state(self, value):

        """
        Purpose: Set state of all tasks of the current stage.

        :arguments: String
        """
        if value not in states.state_numbers.keys():
            raise ValueError(   obj=self._uid, 
                                attribute='set_tasks_state', 
                                expected_value = states.state_numbers.keys(), 
                                actual_value = value)

        for task in self._tasks:
            task.state = value


    def _check_stage_complete(self):

        """
        Purpose: Check if all tasks of the current stage have completed, i.e., are in either DONE or FAILED state.
        """

        try:

            for task in self._tasks:
                if task.state not in [states.DONE, states.FAILED]:
                    return False
                    
            return True

        except Exception, ex:

            print 'Task state evaluation failed'
            raise Error(text=ex)


    def _validate_tasks(self, tasks):

        """
        Purpose: Validate whether the 'tasks' is of type set. Validate the description of each Task.

        Details: This method is to be called before the resource request is placed. Currently, this method is called 
        when tasks are added to the stage.
        """        

        if not isinstance(tasks, set):

            if not isinstance(tasks, list):
                tasks = set([tasks])
            else:
                tasks = set(tasks)

        if not tasks:
            raise TypeError(expected_type=Task, actual_type=type(tasks))

        for t in tasks:

            if not isinstance(t, Task):
                raise TypeError(expected_type=Task, actual_type=type(t))

            t._validate()

        return tasks

    def _validate(self):

        """
        Purpose: Validate that the state of the current Stage is 'DESCRIBED' (user has not meddled with it). Also 
        validate that the current Stage contains Tasks

        Details: This method is to be called before the resource request is placed. Currently, this method is called
        when the parent Pipeline is validated.
        """

        if self._state is not states.INITIAL:
            
            raise ValueError(   obj=self._uid, 
                                attribute='state', 
                                expected_value=states.INITIAL,
                                actual_value=self._state)

        if not self._tasks:

            raise MissingError( obj=self._uid,
                                missing_attribute='tasks')

    # ------------------------------------------------------------------------------------------------------------------