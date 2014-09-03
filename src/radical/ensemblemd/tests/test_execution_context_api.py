""" Tests cases
"""
import os
import sys
import unittest

from radical.ensemblemd.exceptions import *
from radical.ensemblemd.tests.helpers import *

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
        from radical.ensemblemd import SingleClusterEnvironment
        from radical.ensemblemd import MultiClusterEnvironment

    #-------------------------------------------------------------------------
    #
    def test__single_cluster_environment_api(self):

        from radical.ensemblemd import SingleClusterEnvironment

        sec = SingleClusterEnvironment(
            resource="localhost", 
            cores=1, 
            walltime=1
        )

        try: 
            sec.run("wrong_type")
        except Exception, ex:
            test_exception(exception=ex, expected_type=TypeError)