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

    def failUnless(self, expr):
        # St00pid speling.
        return self.assertTrue(expr)

    def failIf(self, expr):
        # St00pid speling.
        return self.assertFalse(expr)

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