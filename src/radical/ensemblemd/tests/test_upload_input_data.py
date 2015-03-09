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

# -------------------------------------------------------------------------
#
class _TestUploadInputData_Pattern(Pipeline):

    def __init__(self, instances, upload_directives, checksum_inputfile, download_output):
        Pipeline.__init__(self, instances)
        self._upload_directives = upload_directives
        self._checksum_inputfile = checksum_inputfile
        self._download_output = download_output

    def step_1(self, instance):
        k = Kernel(name="misc.chksum")
        k.arguments            = ["--inputfile={0}".format(self._checksum_inputfile), "--outputfile={0}".format(self._download_output)]
        k.upload_input_data    = self._upload_directives
        k.download_output_data = self._download_output
        return k

#-----------------------------------------------------------------------------
#
class TestUploadInputData(unittest.TestCase):

    def setUp(self):
        # clean up fragments from previous tests
        for fl in glob.glob("./CHKSUM_*"):
            os.remove(fl)

    def tearDown(self):
        # clean up after ourselves
        try:
            for fl in glob.glob("./CHKSUM_*"):
                os.remove(fl)
        except Exception:
            pass

    #-------------------------------------------------------------------------
    #
    def test__upload_input_data_single(self):
        """Check if we can upload input data from a location on the host running these tests - single input.
        """
        cluster = SingleClusterEnvironment(
            resource="localhost",
            cores=1,
            walltime=5
        )

        test = _TestUploadInputData_Pattern(
            instances=1,
            upload_directives="/etc/passwd",
            checksum_inputfile="passwd",
            download_output="CHKSUM_1"
        )
        cluster.allocate()
        cluster.run(test)

        f = open("./CHKSUM_1")
        csum, fname = f.readline().split()
        assert "passwd" in fname
        f.close()
        os.remove("./CHKSUM_1")

    #-------------------------------------------------------------------------
    #
    def test__upload_input_data_single_rename(self):
        """Check if we can upload input data from a location on the host running these tests - single input with rename.
        """
        cluster = SingleClusterEnvironment(
            resource="localhost",
            cores=1,
            walltime=15
        )

        test = _TestUploadInputData_Pattern(
            instances=1,
            upload_directives="/etc/passwd > input",
            checksum_inputfile="input",
            download_output="CHKSUM_2"
        )
        cluster.allocate()
        cluster.run(test)

        f = open("./CHKSUM_2")
        csum, fname = f.readline().split()
        assert "input" in fname
        f.close()
        os.remove("./CHKSUM_2")

    #-------------------------------------------------------------------------
    #
    def test__upload_input_data_multi(self):
        """Check if we can upload input data from a location on the host running these tests - multiple input.
        """
        cluster = SingleClusterEnvironment(
            resource="localhost",
            cores=1,
            walltime=15
        )

        test = _TestUploadInputData_Pattern(
            instances=1,
            upload_directives=["/etc/passwd", "/etc/group"],
            checksum_inputfile="passwd",
            download_output="CHKSUM_3"
        )
        cluster.allocate()
        cluster.run(test)

        f = open("./CHKSUM_3")
        csum, fname = f.readline().split()
        assert "passwd" in fname
        f.close()
        os.remove("./CHKSUM_3")

    #-------------------------------------------------------------------------
    #
    def test__upload_input_data_multi_rename(self):
        """Check if we can upload input data from a location on the host running these tests - multiple input with rename.
        """
        cluster = SingleClusterEnvironment(
            resource="localhost",
            cores=1,
            walltime=15
        )

        test = _TestUploadInputData_Pattern(
            instances=1,
            upload_directives=["/etc/group > input_g", "/etc/passwd > input_p"],
            checksum_inputfile="input_p",
            download_output="CHKSUM_4"
        )
        cluster.allocate()
        cluster.run(test)

        f = open("./CHKSUM_4")
        csum, fname = f.readline().split()
        assert "input_p" in fname
        f.close()
        os.remove("./CHKSUM_4")
