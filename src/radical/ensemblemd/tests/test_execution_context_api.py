""" Tests cases
"""
import os
import sys
import glob
import unittest

from radical.ensemblemd.exceptions import TypeError
from radical.ensemblemd.tests.helpers import _exception_test_helper

#-----------------------------------------------------------------------------
#
class ExecutionContextAPITestCases(unittest.TestCase):
    # silence deprecation warnings under py3

    def setUp(self):
        # clean up fragments from previous tests
        for fl in glob.glob("./CHKSUM_*"):
            os.remove(fl)
    def tearDown(self):
        # clean up after ourselves
        for fl in glob.glob("./CHKSUM_*"):
            os.remove(fl)

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
        """ Test the single cluster environment API.
        """

        from radical.ensemblemd import SingleClusterEnvironment

        sec = SingleClusterEnvironment(
            resource="localhost",
            cores=1,
            walltime=1
        )

        try:
            sec.allocate()
            sec.run("wrong_type")
            assert False, "TypeError execption expected."
        except Exception, ex:
            pass
