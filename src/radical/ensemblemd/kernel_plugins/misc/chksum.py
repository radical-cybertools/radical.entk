#!/usr/bin/env python

"""A kernel that creates a new ASCII file with a given size and name.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
from copy import deepcopy

from radical.ensemblemd.exceptions import ArgumentError
from radical.ensemblemd.kernel_plugins.kernel_base import KernelBase

# ------------------------------------------------------------------------------
# 
_KERNEL_INFO = {
    "name":         "misc.chksum",
    "description":  "Calculates an SHA1 checksum for a given file.",
    "arguments":   {"--inputfile=":     
                        {
                        "mandatory": True,
                        "description": "The input file."
                        },
                    "--outputfile=":     
                        {
                        "mandatory": True,
                        "description": "The output file containing SHA1 sum."
                        },
                    },
    "machine_configs": 
    {
        "localhost": {
            "environment"   : None,
            "pre_exec"      : None,
            "executable"    : "shasum",
            "uses_mpi"      : "False"
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

        if sys.platform == 'darwin':
            # On OS X (all BSDs?) the tool is called shasum
            chksum_tool = 'shasum'
        else:
            # On Linux the tool is called sha1sum
            chksum_tool = 'sha1sum'

        executable = "/bin/bash"
        arguments  = ['-l', '-c', '{0} {1} > {2}'.format(
            chksum_tool,
            self.get_arg("--inputfile="),
            self.get_arg("--outputfile="))
        ]

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
            "pre_exec"    : [download_commands],
            "post_exec"   : None,
            "executable"  : executable,
            "arguments"   : arguments,
            "use_mpi"     : cfg["uses_mpi"],
            "input_data"  : {
                "upload"      : self._upload_input_data,
                "download"    : self._download_input_data,
                "copy"        : self._copy_input_data,
                "link"        : self._link_input_data,
            }
        }
