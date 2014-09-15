#!/usr/bin/env python

"""The NAMD molecular dynamics toolkit. (http://www.ks.uiuc.edu/Research/namd/).
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
    "name":            "md.namd",
    "description":     "The NAMD molecular dynamics toolkit (http://www.ks.uiuc.edu/Research/namd/)",
    "arguments":       "*",  # "*" means arguments are not evaluated and just passed through to the kernel.
    "machine_configs": 
    {
        "localhost": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : [],
            "executable"    : "namd2",
            "uses_mpi"      : "False"
        },
        "stampede.tacc.utexas.edu": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : ["module load TACC && module load namd/2.9"],
            "executable"    : "/opt/apps/intel13/mvapich2_1_9/namd/2.9/bin/namd2",
            "uses_mpi"      : "True"
        },
        "india.futuregrid.org": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : [],
            "executable"    : "/N/u/marksant/software/bin/namd2",
            "uses_mpi"      : "True"
        },
        "archer.ac.uk": {
            "environment"   : {"FOO": "bar"},
            "pre_exec"      : ["module load namd"],
            "executable"    : "/usr/local/packages/namd/namd-2.9/bin/namd2",
            "uses_mpi"      : "True"
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
    def _get_kernel_description(self, resource_key):
        """(PRIVATE) Implements parent class method. Returns the kernel
           description as a dictionary.
        """
        if resource_key not in _KERNEL_INFO["machine_configs"]:
            raise NoKernelConfigurationError(kernel_name=_KERNEL_INFO["name"], resource_key=resource_key)

        # Translate upload directives into cURL command(s)
        download_commands = ""
        for download in self._download_input_data:

            # see if a rename is requested
            dl = download.split(">")
            if len(dl) == 1:
                # no rename
                 cmd = "curl -L {0}".format(dl[0].strip())
            elif len(dl) == 2:
                 cmd = "curl -L {0} -o {1}".format(dl[0].strip(), dl[1].strip())
            else:
                # error
                raise Exception("Invalid transfer directive %s" % download)
           
            download_commands += "{0}".format(cmd)
            if download != self._download_input_data[-1]:
                download_commands += " && "

        cfg = _KERNEL_INFO["machine_configs"][resource_key]

        return {
            "environment" : cfg["environment"],
            "pre_exec"    : download_commands,
            "post_exec"   : None,
            "executable"  : cfg["executable"],
            "arguments"   : self.get_raw_args(),
            "use_mpi"     : cfg["uses_mpi"],
            "input_data"  : {
                "upload"      : self._upload_input_data,
                "download"    : self._download_input_data,
                "copy"        : self._copy_input_data,
                "link"        : self._link_input_data,
            }
        }
