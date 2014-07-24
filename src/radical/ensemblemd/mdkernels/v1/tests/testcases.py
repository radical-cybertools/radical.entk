""" Tests cases
"""
import os
import sys
import unittest


#-----------------------------------------------------------------------------
#
class MDKernelTestCases(unittest.TestCase):
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
    def test__loader(self):
        """ Test the _KernelLoader.
        """
        from radical.ensemblemd.mdkernels.v1.loader import kerneldict

        assert kerneldict != {}
        assert "TEST" in kerneldict, "Missing key in kernel dict"
        assert "localhost" in kerneldict["TEST"]
        assert "test.host.02" in kerneldict["TEST"]

    #-------------------------------------------------------------------------
    #
    def test__binding(self):
        """ Test the abstract MDTask -> resource binding.
        """
        from radical.ensemblemd.mdkernels.v1 import MDTaskDescription

        r1 = MDTaskDescription()
        r1.kernel = "TEST"
        r1.arguments = ["-f"]

        r1_bound = r1.bind(resource="localhost")
        print r1_bound.pre_exec
        assert r1_bound.pre_exec   == ["/bin/echo -n TEST:localhost"]
        assert r1_bound.executable == "/bin/hostname"
        assert r1_bound.arguments   == r1.arguments
        assert r1_bound.resource   == "localhost"
        #assert r1_bound.input_data
        #assert r1_bound.output_data

    #-------------------------------------------------------------------------
    #
    def test__copy_local_data(self):
        """ Test if copying of local data is handled properly.
        """
        from radical.ensemblemd.mdkernels.v1 import MDTaskDescription

        r1 = MDTaskDescription()
        r1.kernel = "TEST"
        r1.arguments = ["-f"]
        r1.copy_local_input_data = ["file1", "file2", "file3"]

        r1_bound = r1.bind(resource="localhost")

        assert r1_bound.pre_exec == [u'/bin/echo -n TEST:localhost', 'cp file1 .', 'cp file2 .', 'cp file3 .']

    #-------------------------------------------------------------------------
    #
    def test__cleanup_local_data(self):
        """ Test if copying of local data is handled properly.
        """
        from radical.ensemblemd.mdkernels.v1 import MDTaskDescription

        r1 = MDTaskDescription()
        r1.kernel = "TEST"
        r1.arguments = ["-f"]
        r1.copy_local_input_data = ["file1", "file2", "file3"]
        r1.purge = True

        r1_bound = r1.bind(resource="localhost")

        assert r1_bound.pre_exec == [u'/bin/echo -n TEST:localhost', 'cp file1 .', 'cp file2 .', 'cp file3 .']
        assert r1_bound.post_exec == ['rm -rf *']

