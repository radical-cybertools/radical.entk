import radical.utils as ru
from radical.entk.exceptions import *
from radical.entk.stage.stage import Stage

class Pipeline(object):

    def __init__(self, stages, name, resource=None):

        self._uid       = ru.generate_id('radical.entk.pipeline')
        self._stages    = stages
        self._name      = name
        self._resource  = resource

        self._state     = 'New'

        self.validate_args()

        # To change states
        self._stage_count = len(self._stages)


    def validate_args(self):

        if not isinstance(self._stages, list):
            self._stages = [self._stages]

        for val in self._stages:
            if not isinstance(val, Stage):
                raise TypeError(expected_type=Stage, actual_type=type(val))
    # -----------------------------------------------
    # Getter functions
    # -----------------------------------------------

    @property
    def name(self):
        return self._name
    
    @property
    def stages(self):
        return self._stages

    @property
    def state(self):
        return self._state
    
    @property
    def resource(self):
        return self._resource
    
    # -----------------------------------------------


    # -----------------------------------------------
    # Setter functions
    # -----------------------------------------------

    @stages.setter
    def stages(self, values):

        self._stages = values
        self.validate_args()
    

    @resource.setter
    def resource(self, value):
        self._resource = value

    def add_stages(self, stages):

        if not isinstance(stages, list):
            stages = [stages]

        self._stages.extend(stages)
        self._stage_count = len(self._stages)


    def remove_stages(self, stage_names):

        if not isinstance(stage_names, list):
            stage_names = [stage_names]

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
