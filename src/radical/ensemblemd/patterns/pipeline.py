#!/usr/bin/env python

"""This module defines and implements the Pipeline class.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.exceptions import NotImplementedError
from radical.ensemblemd.execution_pattern import ExecutionPattern
from radical.ensemblemd.utils import dataframes_from_profile_dict

PATTERN_NAME = "Pipeline"


# ------------------------------------------------------------------------------
#
class Pipeline(ExecutionPattern):
    """ The pipeline pattern.

            .. image:: ../../images/pipeline_pattern.*
               :width: 300pt
    """

    #---------------------------------------------------------------------------
    #
    def __init__(self, instances=1,steps=1):
        """Creates a new Pipeline instance.
        """
        self._instances = instances
        self._steps = steps

        self._execution_profile = None

        super(Pipeline, self).__init__()


    #---------------------------------------------------------------------------
    #
    @property
    def instances(self):
        """Returns the instances of the pipeline.
        """
        return self._instances

    #---------------------------------------------------------------------------
    #
    @property
    def steps(self):
        """Returns the instances of the pipeline.
        """
        return self._steps

    #---------------------------------------------------------------------------
    #
    @property
    def name(self):
        """Returns the name of the pattern.
        """
        return PATTERN_NAME

    #---------------------------------------------------------------------------
    #
    @property
    def execution_profile_dict(self):
        """Returns the execution profile as a Python dictionary after the
           pattern has finished running, 'None' otheriwse.
        """
        return self._execution_profile

    #---------------------------------------------------------------------------
    #
    @property
    def execution_profile_dataframe(self):
        """Returns the execution profile as a PANDAS DataFrame after the
           pattern has finished running, 'None' otheriwse.

           Note that 'None' is also returned if PANDAS is not installed. 
        """
        return dataframes_from_profile_dict(self._execution_profile)

    #---------------------------------------------------------------------------
    #
    def step_1(self, column):
        """The first step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_1",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def step_2(self, column):
        """The second step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_2",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def step_3(self, column):
        """The third step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_3",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def step_4(self, column):
        """The fourth step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_4",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def step_5(self, column):
        """The fifth step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_5",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def step_6(self, column):
        """The sixth step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_6",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def step_7(self, column):
        """The seventh step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_7",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def step_8(self, column):
        """The eighth step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_8",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def step_9(self, column):
        """The ninth step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_9",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def step_10(self, column):
        """The tenth step of the pipeline.
        """
        raise NotImplementedError(
          method_name="step_10",
          class_name=type(self))
