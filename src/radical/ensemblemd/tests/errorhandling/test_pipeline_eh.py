""" Tests cases
"""
import os
import sys
import glob
import unittest

from radical.ensemblemd import Kernel
from radical.ensemblemd import Pipeline
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment


# ------------------------------------------------------------------------------
#
class _FaultyPattern(Pipeline):

    def __init__(self, instances):
        Pipeline.__init__(self, instances)

    def step_1(self, instance):

        k = Kernel(name="misc.chksum")
        k.arguments            = ["--inputfile=UTF-8-demo.txt", "--outputfile=checksum{0}.sha1".format(instance)]
        k.download_input_data  = "htpttpt://malformed.url"
        k.download_output_data = "checksum{0}.sha1".format(instance)
        return k


#-----------------------------------------------------------------------------
#
class PipelineErrorHandlingTests(unittest.TestCase):

    def setUp(self):
        # clean up fragments from previous tests
        pass

    def tearDown(self):
        # clean up after ourselves
        pass

    #-------------------------------------------------------------------------
    #
    def test__throw_on_malformed_kernel(self):
        """Test if an exception is thrown in case things go wrong in the Pipline pattern.
        """
        try:
            # Create a new static execution context with one resource and a fixed
            # number of cores and runtime.
            cluster = SingleClusterEnvironment(
                resource="localhost",
                cores=1,
                walltime=1,
                username=None,
                allocation=None
            )

            ccount = _FaultyPattern(instances=1)
            cluster.run(ccount)

            assert False, "Expected exception due to malformed URL in Pattern description."

        except EnsemblemdError, er:
            # Exception should pop up.
            assert True
