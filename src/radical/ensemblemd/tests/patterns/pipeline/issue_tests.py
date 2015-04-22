""" Pipeline issues test cases
"""
import os
import sys
import glob
import unittest

import radical.ensemblemd

#-----------------------------------------------------------------------------
#
class PipelinePatternIssueTests(unittest.TestCase):
    """ All tests for Kernel-related issues.

        Run: NOSE_VERBOSE=2 nosetests radical.ensemblemd.tests.patterns.pipeline.issue_tests
    """
    def setUp(self):
        # clean up fragments from previous tests
        pass

    def tearDown(self):
        # clean up after ourselves
        pass

    #-------------------------------------------------------------------------
    #
    def test__issue_22(self):
        """ Issue https://github.com/radical-cybertools/radical.ensemblemd/issues/22
        """
        # ------------------------------------------------------------------------------
        #
        class Test22(radical.ensemblemd.Pipeline):

            def __init__(self, instances):
                radical.ensemblemd.Pipeline.__init__(self, instances)

            def step_1(self, instance):
                """This step downloads a sample UTF-8 file from a remote websever and
                   calculates the SHA1 checksum of that file. The checksum is written
                   to an output file and tranferred back to the host running this
                   script.
                """
                k = radical.ensemblemd.Kernel(name="misc.chksum")
                k.arguments            = ["--inputfile=input.dat", "--outputfile=checksum{0}.sha1".format(instance)]
                k.upload_input_data    = "non/existing/file > input.dat"
                k.download_output_data = "checksum{0}.sha1".format(instance)
                return k

        cluster = radical.ensemblemd.SingleClusterEnvironment(
            resource="localhost",
            cores=1,
            walltime=2,
            username=None,
            allocation=None
        )

        # Allocate the resources.
        cluster.allocate()

        try:
            ccount = Test22(instances=1)
            cluster.run(ccount)
        except radical.ensemblemd.EnsemblemdError, er:
            assert "File does not exist" in str(er), str(er)
