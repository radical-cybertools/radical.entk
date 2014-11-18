""" Tests cases
"""
import os
import sys
import unittest


#-----------------------------------------------------------------------------
#
class ExecutionPatternAPITestCases(unittest.TestCase):
    # silence deprecation warnings under py3

    def setUp(self):
        # clean up fragments from previous tests
        pass

    def tearDown(self):
        # clean up after ourselves 
        pass

    #-------------------------------------------------------------------------
    #
    def test__import(self):
        """ Tests whether we can import the various pattern classes.
        """
        from radical.ensemblemd import Pipeline
        from radical.ensemblemd import SimulationAnalysisLoop


    #-------------------------------------------------------------------------
    #
    def test__pipeline_pattern_api(self):
        """ Tests the Pipeline pattern API.
        """

        from radical.ensemblemd import Pipeline

        dp = Pipeline()
        assert dp.name == "Pipeline"


    #-------------------------------------------------------------------------
    #
    def test__simulation_analysis_loop_pattern_api(self):
        """ Tests the Pipeline execution pattern API.
        """

        from radical.ensemblemd import SimulationAnalysisLoop

        dp = SimulationAnalysisLoop(iterations=1)
        assert dp.name == "SimulationAnalysisLoop"