""" Kernel issues tests cases
"""
import os
import sys
import glob
import unittest

import radical.ensemblemd

#-----------------------------------------------------------------------------
#
class KernelIssueTests(unittest.TestCase):
    """ All tests for Kernel-related issues.

        Run: NOSE_VERBOSE=2 nosetests radical.ensemblemd.tests.kernels.issue_tests
    """
    def setUp(self):
        # clean up fragments from previous tests
        pass

    def tearDown(self):
        # clean up after ourselves
        pass

    #-------------------------------------------------------------------------
    #
    def test__issue_00(self):
        """ Issue https://github.com/radical-cybertools/radical.ensemblemd/issues/00
        """
        pass

    #-------------------------------------------------------------------------
    #
    def test__issue_18(self):
        """ Issue https://github.com/radical-cybertools/radical.ensemblemd/issues/18
        """
        k = radical.ensemblemd.Kernel(name="md.coco")

        try:
            k.cores = "2"
        except radical.ensemblemd.exceptions.TypeError:
            pass
        except Exception, e:
            self.fail('Unexpected exception thrown: %s' % type(e))
        else:
            self.fail('ExpectedException not thrown')
