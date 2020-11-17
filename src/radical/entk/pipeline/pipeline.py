
__copyright__ = 'Copyright 2014-2020, http://radical.rutgers.edu'
__license__   = 'MIT'

import radical.utils as ru
from radical.entk.exceptions import TypeError, ValueError, MissingError, EnTKError
from radical.entk.stage.stage import Stage
import threading
from radical.entk import states


class Pipeline(object):

    """
    A pipeline represents a collection of objects that have a linear temporal
    execution order.  In this case, a pipeline consists of multiple 'Stage'
    objects. Each ```Stage_i``` can execute only after all stages up to
    ```Stage_(i-1)``` have completed execution.

    """

    def __init__(self):

        self._uid  = ru.generate_id('pipeline.%(counter)04d', ru.ID_CUSTOM)
        self._name = None

        self._stages = list()

        self._state = states.INITIAL

        # Keep track of states attained
        self._state_history = [states.INITIAL]

        # To keep track of current state
        self._stage_count = len(self._stages)
        self._cur_stage = 0

        # Lock around current stage
        self._lock = threading.Lock()

        # To keep track of termination of pipeline
        self._completed_flag = threading.Event()


    # --------------------------------------------------------------------------
    # Getter functions
    # --------------------------------------------------------------------------

    @property
    def name(self):
        """
        Name of the pipeline useful for bookkeeping and to refer to this 
        pipeline while data staging. Do not use a ',' or '_' in an object's name.

        :getter: Returns the name of the pipeline
        :setter: Assigns the name of the pipeline
        :type: String
        """
        return self._name

    @property
    def stages(self):
        """
        Stages of the list

        :getter: Returns the stages in the current Pipeline
        :setter: Assigns the stages to the current Pipeline
        :type: List
        """

        return self._stages

    @property
    def state(self):
        """
        Current state of the pipeline

        :getter: Returns the state of the current pipeline
        :type: String
        """

        return self._state

    @property
    def uid(self):
        """
        Unique ID of the current pipeline

        :getter: Returns the unique id of the current pipeline
        :type: String
        """
        return self._uid


    @property
    def luid(self):
        """
        Unique ID of the current pipeline (fully qualified).
        For the pipeline class, his is an alias to `uid`.

        example:
            >>> pipeline.luid
            pipe.0001

        :getter: Returns the fully qualified uid of the current pipeline
        :type: String
        """
        if self.name: return self.name
        else        : return self.uid


    @property
    def lock(self):
        """
        Returns the lock over the current Pipeline

        :return: Lock object
        """

        return self._lock

    @property
    def completed(self):
        """
        Returns whether the Pipeline has completed

        :return: Boolean
        """
        return self._completed_flag.is_set()

    @property
    def current_stage(self):
        """
        Returns the current stage being executed

        :return: Integer
        """

        return self._cur_stage

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

    @stages.setter
    def stages(self, value):

        self._stages = self._validate_entities(value)

        self._stage_count = len(self._stages)
        if self._cur_stage == 0:
            self._cur_stage = 1

    @state.setter
    def state(self, value):
        if isinstance(value, str):
            if value in list(states._pipeline_state_values.keys()):
                self._state = value

                # We add SUSPENDED to state history in suspend()
                if self._state != states.SUSPENDED:
                    self._state_history.append(value)
            else:
                raise ValueError(obj=self._uid,
                                 attribute='state',
                                 expected_value=list(states._pipeline_state_values.keys()),
                                 actual_value=value)
        else:
            raise TypeError(expected_type=str, actual_type=type(value))


    # --------------------------------------------------------------------------
    #
    def add_stages(self, value):
        """
        Appends stages to the current Pipeline

        :argument: List of Stage objects
        """
        stages = self._validate_entities(value)

        self._stages.extend(stages)
        self._stage_count = len(self._stages)
        if self._cur_stage == 0:
            self._cur_stage = 1

        for stage in stages:
            stage.parent_pipeline['uid']  = self._uid
            stage.parent_pipeline['name'] = self._name

            for task in stage.tasks:
                task.parent_pipeline['uid']  = self._uid
                task.parent_pipeline['name'] = self._name


    def to_dict(self):
        """
        Convert current Pipeline (i.e. its attributes) into a dictionary

        :return: python dictionary
        """

        pipeline_desc_as_dict = {

            'uid': self._uid,
            'name': self._name,
            'state': self._state,
            'state_history': self._state_history,
            'completed': self._completed_flag.is_set()
        }

        return pipeline_desc_as_dict

    def from_dict(self, d):
        """
        Create a Pipeline from a dictionary. The change is in inplace.

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
                if d['state'] in list(states._pipeline_state_values.keys()):
                    self._state = d['state']
                else:
                    raise ValueError(obj=self._uid,
                                     attribute='state',
                                     expected_value=list(states._pipeline_state_values.keys()),
                                     actual_value=d['state'])
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

        if 'completed' in d:
            if isinstance(d['completed'], bool):
                if d['completed']:
                    self._completed_flag.set()
            else:
                raise TypeError(entity='completed', expected_type=bool,
                                actual_type=type(d['completed']))

    # --------------------------------------------------------------------------
    #
    def suspend(self):
        '''
        Pause execution of the pipeline: stages and tasks that are executing
        will continue to execute, but no new stages and tasks will be eligible
        for execution until `resume()` is called.

         - The `suspend()` method can not be called on a suspended or completed
           pipeline, doing so will result in an exeption.
         - The state of the pipeline will be set to `SUSPENDED`.
        '''
        if self._state == states.SUSPENDED:
            raise EnTKError(
                'suspend() called on Pipeline %s that is already suspended' % self._uid)

        self._state = states.SUSPENDED
        self._state_history.append(self._state)


    # --------------------------------------------------------------------------
    #
    def resume(self):
        '''
        Continue execution of paused stages and tasks.

         - The `resume()` method can only be called on a suspended pipeline, an
           exception will be raised if that condition is not met.
         - The state of a resumed pipeline will be set to the state the pipeline
           had before suspension.
        '''
        if self._state != states.SUSPENDED:
            raise EnTKError('Cannot resume Pipeline %s: not suspended [%s] [%s]'
                    % (self._uid, self._state, self._state_history))

        self._state = self._state_history[-2]
        self._state_history.append(self._state)


    # --------------------------------------------------------------------------
    # Private methods
    # --------------------------------------------------------------------------

    def _increment_stage(self):
        """
        Purpose: Increment stage pointer. Also check if Pipeline has completed.
        """

        try:

            if self._cur_stage < self._stage_count:
                self._cur_stage += 1
            else:
                self._completed_flag.set()

        except Exception as ex:
            raise EnTKError(msg=ex) from ex

    def _decrement_stage(self):
        """
        Purpose: Decrement stage pointer. Reset completed flag.
        """

        try:

            if self._cur_stage > 0:
                self._cur_stage -= 1
                self._completed_flag = threading.Event()  # reset

        except Exception as ex:
            raise EnTKError(msg=ex) from ex

    @classmethod
    def _validate_entities(self, stages):
        """
        Purpose: Validate whether the argument 'stages' is of list of Stage objects

        :argument: list of Stage objects
        """
        if not stages:
            raise TypeError(expected_type=Stage, actual_type=type(stages))

        if not isinstance(stages, list):
            stages = [stages]

        for value in stages:
            if not isinstance(value, Stage):
                raise TypeError(expected_type=Stage, actual_type=type(value))

        return stages

    def _validate(self):
        """
        Purpose: Validate that the state of the current Pipeline is 'DESCRIBED' (user has not meddled with it). Also
        validate that the current Pipeline contains Stages.
        """

        if self._state is not states.INITIAL:

            raise ValueError(obj=self._uid,
                             attribute='state',
                             expected_value=states.INITIAL,
                             actual_value=self._state)

        if not self._stages:

            raise MissingError(obj=self._uid,
                               missing_attribute='stages')

        for stage in self._stages:
            stage._validate()


# ------------------------------------------------------------------------------

