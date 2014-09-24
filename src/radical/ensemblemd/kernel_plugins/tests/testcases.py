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

        k1 = Kernel(name='misc.mkfile')
        k1.arguments = ["--size=10", "--filename=out"]
        k2 = Kernel(name='misc.mkfile')
        k2.arguments = ["--size=20", "--filename=out"]

        assert k1 != k2
        assert k1.get_arg("--size=") != k2.get_arg("--size=")
        assert k1.get_arg("--filename=") == k2.get_arg("--filename=")
