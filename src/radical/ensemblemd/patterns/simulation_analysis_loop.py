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
    def __init__(self, maxiterations, simulation_instances=1, analysis_instances=1):
        """Creates a new SimulationAnalysisLoop.
 
        **Arguments:**

            * **maxiterations** [`int`]
              The maxiterations parameter determines the maximum number of 
              iterations the simulation-analysis loop is executed. After 
              maxiterations is reached, the loop terminates and `post_loop` is called.

            * **simulation_instances** [`int`]
              The simulation_instances parameter determines the number of independent 
              simulation instances launched for each `simulation_step`. 

            * **analysis_instances** [`int`]
              The analysis_instances parameter determines the number of independent 
              analysis instances launched for each `analysis_step`. 

        """

        super(SimulationAnalysisLoop, self).__init__()
        
    #---------------------------------------------------------------------------
    #
    @property
    def name(self):
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
    def pre_simulation(self,iteration,instance):
        """The :class:`radical.ensemblemd.Kernel` returned by `pre_simulation` 
        is executed once per loop iteration before `simulation_step`.
        
        This step is to divide the input files into smaller files which are
        fed to each of the simulation Compute Units.
        
        **Arguments:**

            * **iteration** [`int`]
              The iteration parameter is a positive integer and references the
              current iteration of the simulation-analysis loop.

            * **instance** [`int`]
              The instance parameter is a positive integer and references the 
              instance of the simulation step, which is in the range
              [1 .. simulation_instances].

        
        **Returns:**

            Implementations of this method **must** return a single 
            :class:`radical.ensemblemd.Kernel` object. An exception is thrown otherwise.

        
        """
        
        raise NotImplementedError(
            method_name="pre_simulation",
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
              [1 .. simulation_instances].

        **Returns:**

            Implementations of this method **must** return a single 
            :class:`radical.ensemblemd.Kernel` object. An exception is thrown otherwise.

        """
        raise NotImplementedError(
            method_name="simulation_step",
            class_name=type(self))


    #---------------------------------------------------------------------------
    #
    def intermediate_step(self, iteration,instance):
        """The :class:`radical.ensemblemd.Kernel` returned by `intermediate_step` 
        is executed once per loop iteration after `simulation_step`.


        This step is required to merge the output of the Simulation step to 
        one concatenated file which is fed to the analysis step as input.
        
        **Arguments:**

            * **iteration** [`int`]
              The iteration parameter is a positive integer and references the
              current iteration of the simulation-analysis loop.

            * **instance** [`int`]
              The instance parameter is a positive integer and references the 
              instance of the simulation step, which is in the range
              [1 .. simulation_instances].

        **Returns:**

            Implementations of this method **must** return a single 
            :class:`radical.ensemblemd.Kernel` object. An exception is thrown otherwise.

        """
        raise NotImplementedError(
          method_name="intermediate_step",
          class_name=type(self))



    #---------------------------------------------------------------------------
    #
    def analysis_step(self, iteration,instance):
        """The :class:`radical.ensemblemd.Kernel` returned by `analysis_step` 
        is executed once per loop iteration after `simulation_step`.

        **Arguments:**

            * **iteration** [`int`]
              The iteration parameter is a positive integer and references the
              current iteration of the simulation-analysis loop.

            * **instance** [`int`]
              The instance parameter is a positive integer and references the 
              instance of the simulation step, which is in the range
              [1 .. simulation_instances].

        **Returns:**

            Implementations of this method **must** return a single 
            :class:`radical.ensemblemd.Kernel` object. An exception is thrown otherwise.

        """
        raise NotImplementedError(
          method_name="analysis_step",
          class_name=type(self))


    #---------------------------------------------------------------------------
    #
    def post_analysis(self,iteration,instance):
        """The :class:`radical.ensemblemd.Kernel` returned by `post_analysis` 
        is executed once per loop iteration after `analysis_step`.

        This step is to perform the updation of the clone files and rewieghting of
        the strucuture file. The output is used as the input for the next iteration.

        **Arguments:**

            * **iteration** [`int`]
              The iteration parameter is a positive integer and references the
              current iteration of the simulation-analysis loop.

            * **instance** [`int`]
              The instance parameter is a positive integer and references the 
              instance of the simulation step, which is in the range
              [1 .. simulation_instances].

        **Returns:**

            Implementations of this method **must** return a single 
            :class:`radical.ensemblemd.Kernel` object. An exception is thrown otherwise.

        """
        raise NotImplementedError(
          method_name="post_analysis",
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
