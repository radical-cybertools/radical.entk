""" Tests cases
"""
import os
import sys
import unittest

from radical.ensemblemd import Kernel
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import SingleClusterEnvironment

# ------------------------------------------------------------------------------
#
class _NopSA(SimulationAnalysisLoop):

    def __init__(self, maxiterations, simulation_instances=1, analysis_instances=1, idle_time=0):
        self.idle_time = idle_time
        SimulationAnalysisLoop.__init__(self, maxiterations, simulation_instances, analysis_instances)

    def pre_loop(self):
        pass

    def simulation_step(self, iteration, instance):
        k = Kernel(name="misc.idle")
        k.arguments = ["--duration={0}".format(self.idle_time)]
        k.upload_input_data = "/etc/passwd"
        #k.download_output_data = "/etc/passwd"
        return k

    def analysis_step(self, iteration, instance):
        k = Kernel(name="misc.idle")
        k.arguments = ["--duration={0}".format(self.idle_time)]
        k.upload_input_data = "/etc/passwd"
        #k.download_output_data = "/etc/passwd"
        return k

    def post_loop(self):
        pass

#-----------------------------------------------------------------------------
#
class SimulationAnalysisLoopPatternProfilingTestCases(unittest.TestCase):
    # silence deprecation warnings under py3

    def setUp(self):
        # clean up fragments from previous tests
        pass

    def tearDown(self):
        # clean up after ourselves
        pass

    #-------------------------------------------------------------------------
    #
    def test__simulation_analysis_loop_profiling(self):
        """ Tests the Pipeline execution pattern API.
        """
        cluster = SingleClusterEnvironment(
            resource="localhost",
            cores=1,
            walltime=30,
            username=None,
            allocation=None
        )
        # wait=True waits for the pilot to become active
        # before the call returns. This is not useful when
        # you want to take advantage of the queueing time /
        # file-transfer overlap, but it's useful for baseline
        # performance profiling of a specific pattern.
        cluster.allocate(wait=True)

        nopsa = _NopSA(
            maxiterations=1,
            simulation_instances=4,
            analysis_instances=4,
            idle_time = 10
        )
        cluster.run(nopsa)

        pdct = nopsa.execution_profile_dict
        dfrm = nopsa.execution_profile_dataframe
