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
        from radical.ensemblemd import DummyPattern

    #-------------------------------------------------------------------------
    #
    def test__dummy_execution_pattern_api(self):
        """ Tests the dummy execution pattern API.
        """

        from radical.ensemblemd import DummyPattern

        dp = DummyPattern()