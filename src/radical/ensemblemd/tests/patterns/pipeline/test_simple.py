""" Tests cases
"""
import os
import sys
import unittest


#-----------------------------------------------------------------------------
#
class PipelinePatternTestCases(unittest.TestCase):
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
        """ Tests whether we can import the pattern class.
        """
        from radical.ensemblemd.patterns.pipeline import Pipeline


    #-------------------------------------------------------------------------
    #
    def test__pattern_name(self):
        """ Tests the pattern name.
        """
        from radical.ensemblemd.patterns.pipeline import Pipeline

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
