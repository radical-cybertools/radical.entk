#!/usr/bin/env python

"""A kernel that creates a new ASCII file with a given size and name.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from copy import deepcopy

from radical.ensemblemd.exceptions import ArgumentError
from radical.ensemblemd.exceptions import NoKernelConfigurationError
from radical.ensemblemd.kernel_plugins.kernel_base import KernelBase

levenshtein = """<<EOF
import sys 

def levenshteinDistance(f1_name, f2_name):

    f1 = open(f1_name, 'r')
    s1 = f1.read()

    f2 = open(f2_name, 'r')
    s2 = f2.read()

    if len(s1) > len(s2):
        s1,s2 = s2,s1
    distances = range(len(s1) + 1)
    for index2,char2 in enumerate(s2):
        newDistances = [index2+1]
        for index1,char1 in enumerate(s1):
            if char1 == char2:
                newDistances.append(distances[index1])
            else:
                newDistances.append(1 + min((distances[index1],
                                             distances[index1+1],
                                             newDistances[-1])))
        distances = newDistances

    f1.close()
    f2.close()

    return distances[-1]

if __name__ == "__main__":
   print levenshteinDistance(sys.argv[1], sys.argv[2])

EOF
"""

# ------------------------------------------------------------------------------
# 
_KERNEL_INFO = {
    "name":         "misc.levenshtein",
    "description":  "Calculates the Levenshtein distance between to strings.",
    "arguments":   {"--inputfile1=":     
                        {
                        "mandatory": True,
                        "description": "The fist input file."
                        },
                    "--inputfile2=":     
                        {
                        "mandatory": True,
                        "description": "The second input file."
                        },
                    "--outputfile=":     
                        {
                        "mandatory": True,
                        "description": "The output file containing the distance value."
                        }
                    },
    "machine_configs": 
    {
        "*": {
            "environment"   : None,
            "pre_exec"      : ["cat >levenshtein.py {0}".format(levenshtein)],
            "executable"    : "/bin/bash",
            "uses_mpi"      : False
        }
    }
}


# ------------------------------------------------------------------------------
# 
class Kernel(KernelBase):

    # --------------------------------------------------------------------------
    #
    def __init__(self):
        """Le constructor.
        """
        super(Kernel, self).__init__(_KERNEL_INFO)

    # --------------------------------------------------------------------------
    #
    @staticmethod
    def get_name():
        return _KERNEL_INFO["name"]

    # --------------------------------------------------------------------------
    #
    def _bind_to_resource(self, resource_key):
        """(PRIVATE) Implements parent class method. 
        """
        if resource_key not in _KERNEL_INFO["machine_configs"]:
            if "*" in _KERNEL_INFO["machine_configs"]:
                # Fall-back to generic resource key
                resource_key = "*"
            else:
                raise NoKernelConfigurationError(kernel_name=_KERNEL_INFO["name"], resource_key=resource_key)

        cfg = _KERNEL_INFO["machine_configs"][resource_key]

        arguments  = ['-l', '-c', 'python levenshtein.py {0} {1} > {2}'.format(
            self.get_arg("--inputfile1="),
            self.get_arg("--inputfile2="),
            self.get_arg("--outputfile="))
        ]

        self._executable  = cfg["executable"] 
        self._arguments   = arguments
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._pre_exec    = cfg["pre_exec"] 
        self._post_exec   = None
