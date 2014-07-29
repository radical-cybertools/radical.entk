""" Tests cases
"""
import os
import sys
import unittest


#-----------------------------------------------------------------------------
#
class ExecutionContextAPITestCases(unittest.TestCase):
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
        """ Tests whether we can import the execution context classes.
        """
        from radical.ensemblemd import StaticExecutionContext
        from radical.ensemblemd import DynamicExecutionContext

    #-------------------------------------------------------------------------
    #
    def test__static_execution_context_api(self):

        from radical.ensemblemd import StaticExecutionContext

        sec = StaticExecutionContext()

        sec.run(1)