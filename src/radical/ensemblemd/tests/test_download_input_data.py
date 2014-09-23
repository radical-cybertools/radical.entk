""" Tests cases
"""
import os
import sys
import glob
import unittest


#-----------------------------------------------------------------------------
#
class TestDownloadInputData(unittest.TestCase):

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
    def test__download_input_data_http(self):
        """Tests whether we can download input data from a remote server 
           via HTTP. 
        """
        pass

    #-------------------------------------------------------------------------
    #
    def test__download_input_data_https(self):
        """Tests whether we can download input data from a remote server 
           via HTTPS. 
        """
        pass