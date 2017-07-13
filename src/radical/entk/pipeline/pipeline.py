import radical.utils as ru
from radical.entk.exceptions import *
from radical.entk.stage.stage import Stage
import threading
from radical.entk import states


class Pipeline(object):

    """
    A pipeline represents a collection of objects that have a linear temporal execution order.
    In this case, a pipeline consists of multiple 'Stage' objects. Each ```Stage_i``` can execute only
    after all stages up to ```Stage_(i-1)``` have completed execution.

    """

    def __init__(self):

        self._uid       = ru.generate_id('radical.entk.pipeline')
        self._stages    = list()
        self._name      = str()

        self._state     = states.INITIAL

        # Keep track of states attained
        self._state_history = []

        # To keep track of current state
        self._stage_count = len(self._stages)
        self._cur_stage = 0

        # Lock around current stage
        self._lock = threading.Lock()

        # To keep track of termination of pipeline
        self._completed_flag = threading.Event()


    def _validate_stages(self, stages):

        if not isinstance(stages, list):
            stages = [stages]

        for val in stages:
            if not isinstance(val, Stage):
                raise TypeError(expected_type=Stage, actual_type=type(val))

        return stages
    # -----------------------------------------------
    # Getter functions
    # -----------------------------------------------

    @property
    def name(self):
        """
        Name of the pipeline useful for bookkeeping and to refer to this pipeline while data staging

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
    def _stage_lock(self):

        """
        Returns the lock over the current Pipeline

        :return: Lock object
        """

        return self._lock
    
    @property
    def _completed(self):

        """
        Returns whether the Pipeline has completed

        :return: Boolean
        """
        return self._completed_flag.is_set()

    @property
    def _current_stage(self):

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
    # -----------------------------------------------


    # -----------------------------------------------
    # Setter functions
    # -----------------------------------------------

    @name.setter
    def name(self, value):
        if isinstance(value,str):
            self._name = value

        else:
            raise TypeError(expected_type=str, actual_type=type(value))

    @stages.setter
    def stages(self, stages):
            
        self._stages = self._validate_stages(stages)

        try:
            self._pass_uid()
            self._stage_count = len(self._stages)
            if self._cur_stage == 0:
                self._cur_stage = 1

        except Exception, ex:
            raise Error(text=ex)


    @state.setter
    def state(self, value):
        if isinstance(value,str):
            self._state = value
            self._state_history.append(value)
        else:
            raise TypeError(expected_type=str, actual_type=type(value)) 

    # -----------------------------------------------

    def add_stages(self, stages):

        """
        
        Appends stages to the current Pipeline

        :argument: List of Stage objects
        """
        stages = self._validate_stages(stages)

        try:
            stages = self._pass_uid(stages)
            self._stages.extend(stages)
            self._stage_count = len(self._stages)
            if self._cur_stage == 0:
                self._cur_stage = 1

        except Exception, ex:
            raise Error(text=ex)

    def remove_stages(self, stage_names):

        """

        Remove stages from the current Pipeline

        :argument: List of stage names as strings
        """

        if not isinstance(stage_names, list):
            stage_names = [stage_names]

        for val in stage_names:
            if not isinstance(val, str):
                raise TypeError(expected_type=str, actual_type=type(val))

        try:

            copy_of_existing_stages = list(self._stages)
            copy_stage_names = list(stage_names)

            for stage in self._stages:

                for stage_name in stage_names:
                
                    if stage.name ==  stage_name:

                        copy_of_existing_stages.remove(stage)
                        copy_stage_names.remove(stage_name)

                stage_names = copy_stage_names

            if len(self._stages) != len(copy_of_existing_stages):

                self._stages = copy_of_existing_stages
                self._stage_count = len(self._stages)

                # Current stage does not change or changes to the 'new' last stage
                self._cur_stage = min(self._cur_stage, self._stage_count)

        except Exception, ex:
            raise Error(text=ex)

    def _pass_uid(self, stages=None):


        """
        Pass current Pipeline's uid to all stages

        :argument: List of Stage objects (optional)
        :return: List of updated Stage objects
        """

        try:

            if stages is None:
                for stage in self._stages:
                    stage._parent_pipeline = self._uid
                    stage._pass_uid()
            else:
                for stage in stages:
                    stage._parent_pipeline = self._uid
                    stage._pass_uid()

                return stages

        except Exception, ex:
            raise Error(text=ex)


    def _increment_stage(self):

        """
        Increment pointer to current stage, also check if Pipeline has completed
        """

        try:

            if self._cur_stage < self._stage_count:
                self._cur_stage+=1
            else:
                self._completed_flag.set()

        except Exception, ex:
            raise Error(text=ex)

    def _decrement_stage(self):

        """
        Decrement pointer to current stage
        """

        try:

            if self._cur_stage > 0:
                self._cur_stage -= 1
                self._completed_flag = threading.Event() # reset
            
        except Exception, ex:
            raise Error(text=ex)


    def to_dict(self):

        """
        Convert current Pipeline into a dictionary

        :return: python dictionary
        """

        pipe_desc_as_dict = {

                                'uid': self._uid,
                                'name': self._name,
                                'state': self._state,
                                'state_history': self._state_history,

                                'stages': self._stages,
                                'stage_count': self._stage_count,
                                'cur_stage': self._cur_stage,
                                'completed': self._completed_flag
                        }

        return pipe_desc_as_dict


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
            self._state = states.UNSCHEDULED


        if 'state_history' in d:
            if isinstance(d['state_history'], list):
                self._state_history = d['state_history']
            else:
                raise TypeError(entity='state_history', expected_type=list, actual_type=type(d['state_history']))

        if 'stages' in d:
            if isinstance(d['stages'], set):
                self._stages = d['stages']
            else:
                raise TypeError(entity='stages', expected_type=set, actual_type=type(d['stages']))

        if 'stage_count' in d:
            if isinstance(d['stage_count'], int):
                self._stage_count = d['stage_count']
            else:
                raise TypeError(entity='stage_count', expected_type=int, actual_type=type(d['stage_count']))

        if 'cur_stage' in d:
            if isinstance(d['cur_stage'], int):
                self._cur_stage = d['cur_stage']
            else:
                raise TypeError(entity='cur_stage', expected_type=int, actual_type=type(d['cur_stage']))

        if 'completed' in d:
            if isinstance(d['completed'], bool):
                self._completed_flag = d['completed']
            else:
                raise TypeError(entity='completed', expected_type=int, actual_type=type(d['completed']))

