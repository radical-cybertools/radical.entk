""" Engine tests cases
"""
import os
import sys
import unittest


#-----------------------------------------------------------------------------
#
class KernelTestCases(unittest.TestCase):
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
        from radical.ensemblemd import Kernel

    #-------------------------------------------------------------------------
    #
    def test_non_singularity(self):
        from radical.ensemblemd import Kernel

        k1 = Kernel(kernel='misc.mkfile', args=["--size 10", "--filename out1"])
        k2 = Kernel(kernel='misc.mkfile', args=["--size 20", "--filename out2"])

        assert k1 != k2
        print k1.get_args()
        print k2.get_args()
        assert k1.get_args() != k2.get_args()
