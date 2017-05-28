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

        self._state     = states.NEW

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
        Name of the pipeline 

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
    def uid(self):

        """
        Unique ID of the current pipeline

        :getter: Returns the unique id of the current pipeline
        :type: String
        """
        return self._uid
    
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
        self._pass_uid()
        self._stage_count = len(self._stages)


    @state.setter
    def state(self, value):
        if isinstance(value,str):
            self._state = value
        else:
            raise TypeError(expected_type=str, actual_type=type(value)) 

    # -----------------------------------------------

    def add_stages(self, stages):

        """
        
        Appends stages to the current Pipeline

        :argument: List of Stage objects
        """

        stages = self._validate_stages(stages)
        stages = self._pass_uid(stages)
        self._stages.extend(stages)
        self._stage_count = len(self._stages)


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

        copy_of_existing_stages = self._stages
        copy_stage_names = stage_names

        for stage in self._stages:

            for stage_name in stage_names:
                
                if stage.name ==  stage_name:

                    copy_of_existing_stages.remove(stage)
                    copy_stage_names.remove(stage)

            stage_names = copy_stage_names

        self._stages = copy_of_existing_stages
        self._stage_count = len(self._stages)

    def _pass_uid(self, stages=None):


        """
        Pass current Pipeline's uid to all stages

        :argument: List of Stage objects (optional)
        :return: List of updated Stage objects
        """

        if stages is None:
            for stage in self._stages:
                stage._parent_pipeline = self._uid
                stage._pass_uid()
        else:
            for stage in stages:
                stage._parent_pipeline = self._uid
                stage._pass_uid()

            return stages


    def _increment_stage(self):

        """
        Increment pointer to current stage, also check if Pipeline has completed
        """

        if self._cur_stage < self._stage_count-1:
            self._cur_stage+=1
        else:
            self._completed_flag.set()

    def _decrement_stage(self):

        """
        Decrement pointer to current stage
        """

        if self._cur_stage > 1:
            self._cur_stage -= 1
            self._completed_flag = threading.Event() # reset
            