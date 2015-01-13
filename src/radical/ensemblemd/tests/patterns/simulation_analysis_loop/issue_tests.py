""" Pipeline issues test cases
"""
import os
import sys
import glob
import unittest

import radical.ensemblemd

#-----------------------------------------------------------------------------
#
class SimulationAnalysisPatternIssueTests(unittest.TestCase):
    """ All tests for Simulation-Analysis-related issues.

        Run: NOSE_VERBOSE=2 nosetests radical.ensemblemd.tests.patterns.simulation_analysis_loop.issue_tests
    """
    def setUp(self):
        # clean up fragments from previous tests
        pass

    def tearDown(self):
        # clean up after ourselves
        pass

    #-------------------------------------------------------------------------
    #
    def test__issue_00(self):
        """ Issue https://github.com/radical-cybertools/radical.ensemblemd/issues/00
        """
        pass
