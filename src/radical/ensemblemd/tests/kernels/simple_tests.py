""" Tests cases
"""
import os
import sys
import glob
import unittest

from radical.ensemblemd import Kernel

#-----------------------------------------------------------------------------
#
class SimpleKernelTests(unittest.TestCase):

    def setUp(self):
        # clean up fragments from previous tests
        pass

    def tearDown(self):
        # clean up after ourselves
        pass

    #-------------------------------------------------------------------------
    #
    def test__amber_kernel(self):
        """Basic test of the AMBER kernel.
        """
        k = Kernel(name="md.amber")


    #-------------------------------------------------------------------------
    #
    def test__coco_kernel(self):
        """Basic test of the CoCo kernel.
        """
        k = Kernel(name="md.coco")

    #-------------------------------------------------------------------------
    #
    def test__gromacs_kernel(self):
        """Basic test of the GROMACS kernel.
        """
        k = Kernel(name="md.gromacs")

    #-------------------------------------------------------------------------
    #
    def test__lsdmap_kernel(self):
        """Basic test of the LSDMAP kernel.
        """
        k = Kernel(name="md.lsdmap")
