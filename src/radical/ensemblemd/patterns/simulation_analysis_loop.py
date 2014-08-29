#!/usr/bin/env python

"""This module defines and implements the Pipeline class.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.exceptions import NotImplementedError
from radical.ensemblemd.execution_pattern import ExecutionPattern

PATTERN_NAME = "SimulationAnalysisLoop"


# ------------------------------------------------------------------------------
#
class SimulationAnalysisLoop(ExecutionPattern):
    
    #---------------------------------------------------------------------------
    #
    def __init__(self, maxiterations, simulation_width=1, analysis_width=1):
        """Creates a new SimulationAnalysisLoop.
 
        **Arguments:**

            * **maxiterations** [`int`]
              The maxiterations parameter determines the maximum number of 
              iterations the simulation-analysis loop is executed. After 
              maxiterations is reached, the loop terminates and `post_loop` is called.

            * **simulation_width** [`int`]
              The simulation_width parameter determines the number of independent 
              simulation instances launched for each `simulation_step`. 

            * **analysis_width** [`int`]
              The analysis_width parameter determines the number of independent 
              analysis instances launched for each `analysis_step`. 

        """

        super(SimulationAnalysisLoop, self).__init__()
        
    #---------------------------------------------------------------------------
    #
    def get_name(self):
        """Returns the name of the pattern.
        """
        return PATTERN_NAME

    #---------------------------------------------------------------------------
    #
    def pre_loop(self):
        """The :class:`radical.ensemblemd.Kernel` returned by `pre_loop` is 
        executed before the main simulation-analysis loop is started.

        It can be used for example to set up structures, initialize 
        experimental environments and so on.

        **Returns:**

            Implementations of this method **must** return a single 
            :class:`radical.ensemblemd.Kernel` object. An exception is thrown otherwise.

        """
        raise NotImplementedError(
            method_name="pre_loop",
            class_name=type(self)) 


    #---------------------------------------------------------------------------
    #
    def simulation_step(self, iteration, instance):
        """The :class:`radical.ensemblemd.Kernel` returned by `simulation_step` 
        is executed once per loop iteration before `analysis_step`.

        **Arguments:**

            * **iteration** [`int`]
              The iteration parameter is a positive integer and references the
              current iteration of the simulation-analysis loop.

            * **instance** [`int`]
              The instance parameter is a positive integer and references the 
              instance of the simulation step, which is in the range
              [1 .. simulation_width].

        **Returns:**

            Implementations of this method **must** return a single 
            :class:`radical.ensemblemd.Kernel` object. An exception is thrown otherwise.

        """
        raise NotImplementedError(
            method_name="simulation_step",
            class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def analysis_step(self, column):
        """The :class:`radical.ensemblemd.Kernel` returned by `analysis_step` 
        is executed once per loop iteration after `simulation_step`.

        **Arguments:**

            * **iteration** [`int`]
              The iteration parameter is a positive integer and references the
              current iteration of the simulation-analysis loop.

            * **instance** [`int`]
              The instance parameter is a positive integer and references the 
              instance of the simulation step, which is in the range
              [1 .. simulation_width].

        **Returns:**

            Implementations of this method **must** return a single 
            :class:`radical.ensemblemd.Kernel` object. An exception is thrown otherwise.

        """
        raise NotImplementedError(
          method_name="step_02",
          class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def post_loop(self):
        """The :class:`radical.ensemblemd.Kernel` returned by `post_loop` is 
        executed after the main simulation-analysis loop has finished.

        It can be used for example to set up structures, initialize 
        experimental environments and so on.

        **Returns:**

            Implementations of this method **must** return a single 
            :class:`radical.ensemblemd.Kernel` object. An exception is thrown otherwise.

        """
        raise NotImplementedError(
          method_name="post_loop",
          class_name=type(self))
