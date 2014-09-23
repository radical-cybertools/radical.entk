""" Tests cases
"""
import os
import sys
import glob
import unittest


#-----------------------------------------------------------------------------
#
class TestDownloadOutputData(unittest.TestCase):

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
    def test__download_output_data(self):
        """Tests whether we can download output data back to the machine 
           executing the tests. 
        """
        pass
