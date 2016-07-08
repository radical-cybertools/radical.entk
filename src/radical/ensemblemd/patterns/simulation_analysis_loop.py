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
    """ The Simulation-Analysis pattern.

            .. image:: ../../images/simulation_analysis_pattern.*
               :width: 300pt

        **Data references**:

        The following placeholders can be used to reference the data during staging of files
        generated in previous steps across different instances:

        * ``$PRE_LOOP`` - References the pre_loop step.
        * ``$PREV_SIMULATION`` - References the previous simulation step with the same instance number.
        * ``$PREV_SIMULATION_INSTANCE_Y`` - References instance Y of the previous simulation step.
        * ``$SIMULATION_ITERATION_X_INSTANCE_Y`` - Refernces instance Y of the simulation step of iteration number X.
        * ``$PREV_ANALYSIS`` - References the previous analysis step with the same instance number.
        * ``$PREV_ANALYSIS_INSTANCE_Y`` - References instance Y of the previous analysis step.
        * ``$ANALYSIS_ITERATION_X_INSTANCE_Y`` - Refernces instance Y of the analysis step of iteration number X.

        For example, to reference the file ``output.dat`` in the output
        directory of the previous analysis step with the same instance can be
        referenced from an analysis step as:

        .. code-block:: python

            def  simulation_stage(self, iteration, instance):
                k = Kernel(name="kernelname")
                k.link_input_data = ["$PREV_ANALYSIS/output.dat]
                # Alternatively: k.copy_input_data to copy the data instead of just linking it
                k.arguments = ["--inputfile1=output.dat"]
                return kg
    """

    #---------------------------------------------------------------------------
    #
    def __init__(self, iterations, simulation_instances=1, analysis_instances=1, adaptive_simulation=False, sim_extraction_script=None):
        """Creates a new SimulationAnalysisLoop.

        **Arguments:**

            * **iterations** [`int`]
              The iterations parameter determines the maximum number of
              iterations the simulation-analysis loop is executed. After
              iterations is reached, the loop terminates and `post_loop` is called.

            * **simulation_instances** [`int`]
              The simulation_instances parameter determines the number of independent
              simulation instances launched for each ` simulation_stage`.

            * **analysis_instances** [`int`]
              The analysis_instances parameter determines the number of independent
              analysis instances launched for each ` analysis_stage`.

        """
        self._iterations = iterations
        self._simulation_instances = simulation_instances
        self._analysis_instances = analysis_instances
        self._adaptive_simulation = adaptive_simulation
        self._sim_extraction_script = sim_extraction_script

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
    @property
    def iterations(self):
        """Returns the maximum number of loop iterations.
        """
        return self._iterations

    #---------------------------------------------------------------------------
    #
    @property
    def simulation_instances(self):
        """Returns the number of simulation instances per loop iteration.
        """
        return self._simulation_instances


    #---------------------------------------------------------------------------
    #
    @simulation_instances.setter
    def simulation_instances(self,sims):
        """Returns the number of simulation instances per loop iteration.
        """
        self._simulation_instances = sims

    #---------------------------------------------------------------------------
    #
    @property
    def analysis_instances(self):
        """Returns the number of analysis instances per loop iteration.
        """
        return self._analysis_instances

    #---------------------------------------------------------------------------
    #
    @property
    def simulation_adaptivity(self):
        return self._simulation_adaptivity
    

    #---------------------------------------------------------------------------
    #
    def pre_loop(self):
        """The :class:`radical.ensemblemd.Kernel` returned by `pre_loop` is
        executed before the main simulation-analysis loop is started.

        It can be used for example to set up structures, initialize
        experimental environments and so on.

        **Returns:**

            Implementations of this method **must** return either a single or a list of
            :class:`radical.ensemblemd.Kernel` object(s). An exception is thrown otherwise.

        """
        #raise NotImplementedError(
        #    method_name="pre_loop",
        #    class_name=type(self))
        return None

    #---------------------------------------------------------------------------
    #
    def simulation_stage(self, iteration, instance):
        """The :class:`radical.ensemblemd.Kernel` returned by ` simulation_stage`
        is executed once per loop iteration before ` analysis_stage`.

        **Arguments:**

            * **iteration** [`int`]
              The iteration parameter is a positive integer and references the
              current iteration of the simulation-analysis loop.

            * **instance** [`int`]
              The instance parameter is a positive integer and references the
              instance of the simulation step, which is in the range
              [1 .. simulation_instances].

        **Returns:**

            Implementations of this method **must** return either a single or a list of
            :class:`radical.ensemblemd.Kernel` object(s). An exception is thrown otherwise.

        """
        raise NotImplementedError(
            method_name=" simulation_stage",
            class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def analysis_stage(self, iteration,instance):
        """The :class:`radical.ensemblemd.Kernel` returned by ` analysis_stage`
        is executed once per loop iteration after `simulation_stage`.

        **Arguments:**

            * **iteration** [`int`]
              The iteration parameter is a positive integer and references the
              current iteration of the simulation-analysis loop.

            * **instance** [`int`]
              The instance parameter is a positive integer and references the
              instance of the simulation step, which is in the range
              [1 .. simulation_instances].

        **Returns:**

            Implementations of this method **must** return either a single or a list of
            :class:`radical.ensemblemd.Kernel` object(s). An exception is thrown otherwise.

        """
        raise NotImplementedError(
          method_name=" analysis_stage",
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
        return None

    @property
    def adaptive_simulation(self):
        return self._adaptive_simulation

    @adaptive_simulation.setter
    def adaptive_simulation(self,value):
        self._adaptive_simulation = value


    @property
    def sim_extraction_script(self):
        return self._sim_extraction_script

    @sim_extraction_script.setter
    def sim_extraction_script(self,script):
        self._sim_extraction_script = script

    def get_new_simulation_instances(self,cu_output):

        if self._sim_extraction_script == None:
            return int(cu_output)

        else:
            f1=open('temp1.dat','w')
            f1.write(cu_output)
            f1.close()
            import os
            import subprocess as sp
            p = sp.Popen('python {0} < temp1.dat'.format(self._sim_extraction_script),stdout=sp.PIPE,stderr=sp.PIPE,shell=True)
            out,err = p.communicate()
            os.remove('temp1.dat')
            return int(out)
