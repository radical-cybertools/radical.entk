""" Tests cases
"""
import os
import sys
import unittest


#-----------------------------------------------------------------------------
#
class AllPairsPatternTestCases(unittest.TestCase):
    # silence deprecation warnings under py3

    def setUp(self):
        # clean up fragments from previous tests
        pass

    def tearDown(self):
        # clean up after ourselves
        pass

    #-------------------------------------------------------------------------
    #
    def test__import(self):
        """ Tests whether we can import the pattern class.
        """
        from radical.ensemblemd import AllPairs


    #-------------------------------------------------------------------------
    #
    def test__pattern_name(self):
        """ Tests the pattern name.
        """
        from radical.ensemblemd import AllPairs

        ElementsSet = range(1,11)

        ap = AllPairs(ElementsSet)
        assert ap.name == "AllPairs"


    #-------------------------------------------------------------------------
    #
    def test__more(self):
        """ Tests the execution pattern API.
        """
        pass
