#!/usr/bin/env python

"""
Hausdorff Distance calculation script for Ensemble MD Toolkit
"""

__author__       = "Ioannis Paraskevakos <i.parask@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"

import math
import os
import ast
from radical.ensemblemd import Kernel
from radical.ensemblemd import AllPairs
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment

from radical.ensemblemd.engine import get_engine
from radical.ensemblemd.kernel_plugins.kernel_base import KernelBase

#-------------------------------------------------------------------------------
#
_KERNEL_INFO_QPC = {
    "name":         "my.qpc",
    "description":  "This Kernel calculates the MQPC alignment.",
    "arguments":   {"--inputfile=":
                        {
                        "mandatory": True,
                        "description": "The first input Trajectory file."
                        },
                    "--filename=":
                        {
                        "mandatory": True,
                        "description": "The output file containing the difference count."
                        }

                    },
    "machine_configs":
    {
        "*": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : [],
            "executable"    : ":",
            "uses_mpi"      : False
        }
    }
}

#-------------------------------------------------------------------------------
#
class MyQPC(KernelBase):

    def __init__(self):
        """Le constructor.
        """
        super(MyQPC, self).__init__(_KERNEL_INFO_QPC)

    @staticmethod
    def get_name():
        return _KERNEL_INFO_QPC["name"]

    def _bind_to_resource(self, resource_key):
        """This function binds the Kernel to a specific resource defined in
           "resource_key".
        """
        if resource_key not in _KERNEL_INFO_QPC["machine_configs"]:
            if "*" in _KERNEL_INFO_QPC["machine_configs"]:
                # Fall-back to generic resource key
                resource_key = "*"
            else:
                raise NoKernelConfigurationError(kernel_name=_KERNEL_INFO_QPC["name"], resource_key=resource_key)

        cfg = _KERNEL_INFO_QPC["machine_configs"][resource_key]

        executable = "/bin/bash"
        arguments  = ['-l', '-c', '{0} cat {1}'.format(cfg["executable"],self.get_arg("--inputfile="))]

        self._executable  = [executable]
        self._arguments   = arguments
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._pre_exec    = cfg["pre_exec"]
        self._post_exec   = None

#-------------------------------------------------------------------------------
#
_KERNEL_INFO_HS = {
    "name":         "my.hausdorff",
    "description":  "This Kernel calculates the Hausdorff distance.",
    "arguments":   {"--dist_file=":
                        {
                        "mandatory": True,
                        "description": "The file that contains the Hausdorff Calculator"
                        },
                    "--inputfile1=":
                        {
                        "mandatory": True,
                        "description": "The first input Trajectory file."
                        },
                    "--inputfile2=":
                        {
                        "mandatory": True,
                        "description": "The second input Trajectory file."
                        },
                    "--outputfile=":
                        {
                        "mandatory": True,
                        "description": "The output file containing the difference count."
                        },

                    },
    "machine_configs":
    {
        "*": {
            "environment"   : None,
            "pre_exec"      : [],
            "executable"    : ":",
            "uses_mpi"      : False
        },
        "xsede.stampede": {
            "environment"   : None,
            "pre_exec"      : ["module load python/2.7.3-epd-7.3.2"],
            "executable"    : ":",
            "uses_mpi"      : False
        }
    }
}

#-------------------------------------------------------------------------------
#
class MyHausdorff(KernelBase):

    def __init__(self):
        """Le constructor.
        """
        super(MyHausdorff, self).__init__(_KERNEL_INFO_HS)

    @staticmethod
    def get_name():
        return _KERNEL_INFO_HS["name"]

    def _bind_to_resource(self, resource_key):
        """This function binds the Kernel to a specific resource defined in
           "resource_key".
        """
        if resource_key not in _KERNEL_INFO_HS["machine_configs"]:
            if "*" in _KERNEL_INFO_HS["machine_configs"]:
                # Fall-back to generic resource key
                resource_key = "*"
            else:
                raise NoKernelConfigurationError(kernel_name=_KERNEL_INFO_HS["name"], resource_key=resource_key)

        cfg = _KERNEL_INFO_HS["machine_configs"][resource_key]
        executable = "python"
        ARG1 = self.get_arg("--inputfile1=")
        ARG2 = self.get_arg("--inputfile2=")

        ARG1 = ast.literal_eval(ARG1)
        ARG2 = ast.literal_eval(ARG2)

        arguments  = [self.get_arg("--dist_file="),"--element_set1"]
        arguments.extend(ARG1)
        arguments.extend(["--element_set2"])
        arguments.extend(ARG2)
        arguments.extend(["--output_file",self.get_arg("--outputfile=")])

        self._executable  = executable
        self._arguments   = arguments
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._pre_exec    = cfg["pre_exec"]
        self._post_exec   = None

# ------------------------------------------------------------------------------
# Register the user-defined kernel with Ensemble MD Toolkit.
get_engine().add_kernel_plugin(MyQPC)
get_engine().add_kernel_plugin(MyHausdorff)

# ------------------------------------------------------------------------------
#
class HausdorffAP(AllPairs):
    """HausdorffAP implements the all pairs pattern described above. It
       inherits from radical.ensemblemd.AllPair, the abstraction
       base class for all All Pairs applications.
    """
    def __init__(self,setelements,windowsize):
        AllPairs.__init__(self, set1elements=setelements,windowsize1=windowsize)

    def set1element_initialization(self,element):
        """The initialization step creates the necessary files that will be
            needed for the comparison over the elements of the set.
        """

        print "Element creation {0}".format(element)
        k = Kernel(name="my.qpc")
        k.arguments = ["--inputfile=traj_flat%d.npz.npy"%(element),
                        "--filename=traj_flat%d.npz.npy"%(element)]
        # When the input data are in a web server use download input data as the example below.
        k.download_input_data = ["http://eceweb1.rutgers.edu/~ip176/traj_flat{0}.npz.npy > traj_flat{0}.npz.npy".format(element)]

        # If the input data are in a local folder use the following
        # k.upload_input_data = ["/<PATH>/<TO>/<FOLDER>/<WITH>/traj_flat{x}.npz.npy > traj_flat{x}.npz.npy"]
        # If the input data are in a folder to the target machine use the following
        # k.link_input_data = ["/<PATH>/<TO>/<FOLDER>/<WITH>/traj_flat{x}.npz.npy > traj_flat{x}.npz.npy"]

        print "Created {0}".format(element)
        return k

    def element_comparison(self, elements1, elements2):
        """In the comparison, we take the previously generated modified trajectory
           and perform a Hausdorff distance calculation between all the unique pairs
           of trajectories
        """
        input_filenames1 = ["traj_flat%d.npz.npy"%(el1) for el1 in elements1]
        input_filenames2 = ["traj_flat%d.npz.npy"%(el2) for el2 in elements2]
        output_filename = "comparison-%03d-%03d.dat"%(elements1[0],elements2[0])

        print "Element Comparison {0} - {1}".format(elements1,elements2)

        k = Kernel(name="my.hausdorff")
        k.arguments            = ["--dist_file=hausdorff_kernel.py",
                                  "--inputfile1={0}".format(input_filenames1),
                                  "--inputfile2={0}".format(input_filenames2),
                                  "--outputfile={0}".format(output_filename)]
        k.upload_input_data = ["hausdorff_kernel.py"]

        # If the input data are in in a web server use the following
        # k.download_input_data = ["/<PATH>/<TO>/<WEB>?<SERVER>/<WITH>/hausdorff_kernel.py > hausdorff_kernel.py"]
        # If the input data are in a folder to the target machine use the following
        # k.link_input_data = ["/<PATH>/<TO>/<FOLDER>/<WITH>/hausdorff_kernel.py > hausdorff_kernel.py"]

        # The result files comparison-x-y.dat are downloaded.
        k.download_output_data = output_filename

        print "Element Comparison Finished {0} - {1}".format(elements1,elements2)

        return k

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.
        cluster = SingleClusterEnvironment(
            resource="local.localhost",
            cores=1, #num of cores
            walltime=30, #minutes
            username=None,
            allocation=None
        )

        # Allocate the resources.
        cluster.allocate()

        # For example the set has 10 elements.
        ElementsSet = range(1,11)
        hausdorff = HausdorffAP(setelements=ElementsSet,windowsize=1)

        cluster.run(hausdorff)

        #Message printed when everything is completed succefully
        print "Hausdorff Distance Files Donwnloaded."
        print "Please check file distance-x-y.dat to find the Hausdorff Distance for Atom Trajectories x and y."


    except EnsemblemdError, er:

        print "Ensemble MD Toolkit Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace

