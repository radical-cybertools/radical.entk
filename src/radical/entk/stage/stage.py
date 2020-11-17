
__copyright__ = 'Copyright 2014-2020, http://radical.rutgers.edu'
__license__   = 'MIT'


import radical.utils as ru
from radical.entk.exceptions import ValueError, TypeError, EnTKError, MissingError
from radical.entk.task.task import Task
from radical.entk import states


class Stage(object):
    """
    A stage represents a collection of objects that have no relative order of
    execution. In this case, a stage consists of a set of 'Task' objects. All
    tasks of the same stage may execute concurrently.
    """

    def __init__(self):

        self._uid = ru.generate_id('stage.%(counter)04d', ru.ID_CUSTOM)
        self._name = None

        self._tasks = set()
        self._state = states.INITIAL

        # Keep track of states attained
        self._state_history = [states.INITIAL]

        # To change states
        self._task_count = len(self._tasks)

        # Pipeline this stage belongs to
        self._p_pipeline = {'uid': None, 'name': None}

        self._post_exec = None

    # ------------------------------------------------------------------------------------------------------------------
    # Getter functions
    # ------------------------------------------------------------------------------------------------------------------

    @property
    def name(self):
        """
        Name of the stage. Do not use a ',' or '_' in an object's name.

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
    def luid(self):
        """
        Unique ID of the current stage (fully qualified).

        example:
            >>> stage.luid
            pipe.0001.stage.0004

        :getter: Returns the fully qualified uid of the current stage
        :type: String
        """
        p_elem = self.parent_pipeline.get('name')
        if not p_elem:
            p_elem = self.parent_pipeline['uid']

        s_elem = self.name
        if not s_elem:
            s_elem = self.uid

        return '%s.%s' % (p_elem, s_elem)


    @property
    def state_history(self):
        """
        Returns a list of the states obtained in temporal order

        :return: list
        """

        return self._state_history


    @property
    def post_exec(self):
        '''
        The post_exec property enables adaptivity in EnTK. post_exec receives a
        Python callable object i.e. function, which will be evaluated when a
        stage is finished.

        Note: if a post_exec callback resumes any suspended pipelines, it MUST
        return a list with the IDs of those pipelines - otherwise the resume
        will not be acted upon.

        Example:

        s1.post_exec = func_post

        def func_post():

            if condition is met:
                s = Stage()
                t = Task()
                t.executable = '/bin/sleep'
                t.arguments = ['30']
                s.add_tasks(t)
                p.add_stages(s)

            else:
                # do nothing
                pass
        '''

        return self._post_exec


    # --------------------------------------------------------------------------
    # Setter functions
    # --------------------------------------------------------------------------

    @name.setter
    def name(self, value):
        if isinstance(value, str):
            if ',' in value:
                raise ValueError(obj=self._uid,
                                attribute='name',
                                actual_value=value,
                                expected_value="Using ',' in an object's name will corrupt the profiling and internal mapping tables")
            else:
                self._name = value
        else:
            raise TypeError(expected_type=str, actual_type=type(value))

    @tasks.setter
    def tasks(self, value):
        self._tasks = self._validate_entities(value)
        self._task_count = len(self._tasks)

    @parent_pipeline.setter
    def parent_pipeline(self, value):
        if isinstance(value, dict):
            self._p_pipeline = value
        else:
            raise TypeError(expected_type=dict, actual_type=type(value))

    @state.setter
    def state(self, value):
        if isinstance(value, str):
            if value in list(states._stage_state_values.keys()):
                self._state = value
                self._state_history.append(value)
            else:
                raise ValueError(obj=self._uid,
                                 attribute='state',
                                 expected_value=list(states._stage_state_values.keys()),
                                 actual_value=value)
        else:
            raise TypeError(expected_type=str, actual_type=type(value))

    @post_exec.setter
    def post_exec(self, value):

        if not callable(value):

            raise TypeError(entity='stage %s branch' % self._uid,
                            expected_type='callable',
                            actual_type=type(value)
                            )

        self._post_exec = value


    # --------------------------------------------------------------------------
    # Public methods
    #
    def add_tasks(self, value):
        """
        Adds task(s) to a Stage by using a set union operation. Every Task
        element is unique, no duplicate is allowed. Existing Task
        won't be added but updated with changes.

        :argument: iterable task object
        """
        tasks = self._validate_entities(value)
        self._tasks.update(tasks)
        self._task_count = len(self._tasks)

        for task in self._tasks:
            task.parent_stage['uid']  = self._uid
            task.parent_stage['name'] = self._name

            task.parent_pipeline['uid']  = self.parent_pipeline['uid']
            task.parent_pipeline['name'] = self.parent_pipeline['name']


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
            if d['uid']:
                self._uid = d['uid']

        if 'name' in d:
            if d['name']:
                self._name = d['name']

        if 'state' in d:
            if isinstance(d['state'], str) or isinstance(d['state'], str):
                if d['state'] in list(states._stage_state_values.keys()):
                    self._state = d['state']
                else:
                    value = d['state']
                    raise ValueError(obj=self._uid,
                                     attribute='state',
                                     expected_value=list(states._stage_state_values.keys()),
                                     actual_value=value)
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
            if isinstance(d['parent_pipeline'], dict):
                self._p_pipeline = d['parent_pipeline']
            else:
                raise TypeError(entity='parent_pipeline', expected_type=dict, actual_type=type(d['parent_pipeline']))

    # ------------------------------------------------------------------------------------------------------------------
    # Private methods
    # ------------------------------------------------------------------------------------------------------------------

    def _set_tasks_state(self, value):
        """
        Purpose: Set state of all tasks of the current stage.

        :arguments: String
        """
        if value not in list(states.state_numbers.keys()):
            raise ValueError(obj=self._uid,
                             attribute='set_tasks_state',
                             expected_value=list(states.state_numbers.keys()),
                             actual_value=value)

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

        except Exception as ex:
            raise EnTKError(msg=ex) from ex

    @classmethod
    def _validate_entities(self, tasks):
        """
        Purpose: Validate whether the 'tasks' is of type set. Validate the description of each Task.
        """

        if not tasks:
            raise TypeError(expected_type=Task, actual_type=type(tasks))

        if not isinstance(tasks, set):

            if not isinstance(tasks, list):
                tasks = set([tasks])
            else:
                tasks = set(tasks)

        for t in tasks:

            if not isinstance(t, Task):
                raise TypeError(expected_type=Task, actual_type=type(t))

        return tasks

    def _validate(self):
        """
        Purpose: Validate that the state of the current Stage is 'DESCRIBED' (user has not meddled with it). Also
        validate that the current Stage contains Tasks
        """

        if self._state is not states.INITIAL:

            raise ValueError(obj=self._uid,
                             attribute='state',
                             expected_value=states.INITIAL,
                             actual_value=self._state)

        if not self._tasks:

            raise MissingError(obj=self._uid,
                               missing_attribute='tasks')

        for task in self._tasks:
            task._validate()


# ------------------------------------------------------------------------------

