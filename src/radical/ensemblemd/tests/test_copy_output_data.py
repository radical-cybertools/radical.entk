""" Tests cases
"""
import os
import sys
import glob
import unittest


#-----------------------------------------------------------------------------
#
class TestCopyOutputData(unittest.TestCase):

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
    def test__copy_output_data(self):
        """Tests whether we can copy output data ta a location on the 
           execution host.
        """
        pass
