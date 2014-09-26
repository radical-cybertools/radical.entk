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

# ------------------------------------------------------------------------------
# 
_KERNEL_INFO = {
    "name":         "md.lsdmap",
    "description":  "Creates a new file of given size and fills it with random ASCII characters.",
    "arguments":   {"--nnfile=":
                        {
                            "mandatory": True,
                            "description": "Nearest neighbor filename"
                        },
                    "--wfile=":
                        {
                            "mandatory": True,
                            "description": "Weight filename"
                        },
                    "--config=":
                        {
                            "mandatory": True,
                            "description": "LSDMap config filename"
                        },
                    "--inputfile=":
                        {
                            "mandatory": True,
                            "description": "Output gro filename"
                        }
                    },
    "machine_configs": 
    {
        "*": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : ["sleep 1"],
            "executable"    : ".",
            "uses_mpi"      : False
        },

        "trestles.sdsc.xsede.org":
        {
            "environment" : {},
            "pre_exec" : ["module load python","module load scipy","module load mpi4py","(test -d $HOME/lsdmap || (git clone https://github.com/jp43/lsdmap.git $HOME/lsdmap && python $HOME/lsdmap/setup.py install --user))","export PATH=$PATH:$HOME/lsdmap/bin","chmod +x $HOME/lsdmap/bin/lsdmap"],
            "executable" : ["/bin/bash"]
        },

        "stampede.tacc.utexas.edu":
        {
            "environment" : {},
            "pre_exec" : ["module load -intel +intel/14.0.1.106","module load python","(test -d $HOME/lsdmap || (git clone https://github.com/jp43/lsdmap.git $HOME/lsdmap))","export PATH=$PATH:$HOME/lsdmap/bin","chmod +x $HOME/lsdmap/bin/lsdmap"],
            "executable" : ["/bin/bash"]
        },

        "archer.ac.uk":
        {
            "environment" : {},
            "pre_exec" : ["module load python && source /fs4/e290/e290/vb224/myenv/bin/activate && module load numpy && module load scipy && module load lsdmap"],
            "executable" : ["/bin/bash"]
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

        executable = "/bin/bash"
        arguments = ['-l', '-c', '{0} run_analyzer.sh {1} {2}'.format(cfg["executable"],
                                                                        self.get_args("--nnfile="),
                                                                        self.get_args("--wfile="))]
       
        self._executable  = executable
        self._arguments   = arguments
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._pre_exec    = cfg["pre_exec"] 
        self._post_exec   = None
