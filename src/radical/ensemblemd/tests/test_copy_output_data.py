""" Tests cases
"""
import os
import sys
import glob
import unittest
import tempfile
import shutil


from radical.ensemblemd import Kernel
from radical.ensemblemd import Pipeline
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment

# -------------------------------------------------------------------------
#
class _TestCopyOutputData_Pattern(Pipeline):

    def __init__(self, instances, output_copy_directives):
        Pipeline.__init__(self, instances)
        self._output_copy_directives = output_copy_directives

    def step_1(self, instance):
        k = Kernel(name="misc.chksum")
        k.arguments            = ["--inputfile=input.txt", "--outputfile=checksum.txt"]
        k.copy_input_data      = ["/etc/passwd > input.txt"]
        k.copy_output_data     = self._output_copy_directives
        return k

#-----------------------------------------------------------------------------
#
class TestCopyOutputData(unittest.TestCase):

    def setUp(self):
        # create a new temporary directory for the copy target
        self._output_dir = tempfile.mkdtemp()

    def tearDown(self):
        # delete temporary directory
        shutil.rmtree(self._output_dir)

    #-------------------------------------------------------------------------
    #
    def test__copy_input_data_single(self):
        """Check if we can copy output data to a different location on the execution host - single input.
        """
        cluster = SingleClusterEnvironment(
            resource="localhost",
            cores=1,
            walltime=5
        )

        test = _TestCopyOutputData_Pattern(
            instances=1,
            output_copy_directives=["checksum.txt > {0}".format(self._output_dir)]
        )
        cluster.allocate()
        cluster.run(test)

        #assert "checksum.txt" in os.listdir(self._output_dir)


    # #-------------------------------------------------------------------------
    # #
    # def test__copy_input_data_single_rename(self):
    #     """Check if we can copy input data from a location on the execution host - single input with rename.
    #     """
    #     cluster = SingleClusterEnvironment(
    #         resource="localhost",
    #         cores=1,
    #         walltime=15
    #     )
    #
    #     test = _TestCopyInputData_Pattern(
    #         instances=1,
    #         copy_directives="/etc/passwd > input",
    #         checksum_inputfile="input",
    #         download_output="CHKSUM_2"
    #     )
    #     cluster.allocate()
    #     cluster.run(test)
    #
    #     f = open("./CHKSUM_2")
    #     csum, fname = f.readline().split()
    #     assert "input" in fname
    #     f.close()
    #     os.remove("./CHKSUM_2")
    #
    # #-------------------------------------------------------------------------
    # #
    # def test__copy_input_data_multi(self):
    #     """Check if we can copy input data from a location on the execution host - multiple input.
    #     """
    #     cluster = SingleClusterEnvironment(
    #         resource="localhost",
    #         cores=1,
    #         walltime=15
    #     )
    #
    #     test = _TestCopyInputData_Pattern(
    #         instances=1,
    #         copy_directives=["/etc/passwd", "/etc/group"],
    #         checksum_inputfile="passwd",
    #         download_output="CHKSUM_3"
    #     )
    #     cluster.allocate()
    #     cluster.run(test)
    #
    #     f = open("./CHKSUM_3")
    #     csum, fname = f.readline().split()
    #     assert "passwd" in fname
    #     f.close()
    #     os.remove("./CHKSUM_3")
    #
    # #-------------------------------------------------------------------------
    # #
    # def test__copy_input_data_multi_rename(self):
    #     """Check if we can copy input data from a location on the execution host - multiple input with rename.
    #     """
    #     cluster = SingleClusterEnvironment(
    #         resource="localhost",
    #         cores=1,
    #         walltime=15
    #     )
    #
    #     test = _TestCopyInputData_Pattern(
    #         instances=1,
    #         copy_directives=["/etc/group > input_g", "/etc/passwd > input_p"],
    #         checksum_inputfile="input_p",
    #         download_output="CHKSUM_4"
    #     )
    #
    #     cluster.allocate()
    #     cluster.run(test)
    #
    #     f = open("./CHKSUM_4")
    #     csum, fname = f.readline().split()
    #     assert "input_p" in fname
    #     f.close()
    #     os.remove("./CHKSUM_4")
