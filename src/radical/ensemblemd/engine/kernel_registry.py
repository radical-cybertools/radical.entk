#!/usr/bin/env python

"""List of all registered kernels.

This registry is used to locate and load kernels. The entries must be
formatted in dotted python module notation.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"


kernel_registry = [
    "radical.ensemblemd.kernel_plugins.md.mmpbsa",
    "radical.ensemblemd.kernel_plugins.md.lsdmap",
    "radical.ensemblemd.kernel_plugins.md.gromacs",
    "radical.ensemblemd.kernel_plugins.md.namd",
    "radical.ensemblemd.kernel_plugins.md.re_exchange",

    "radical.ensemblemd.kernel_plugins.misc.mkfile",
    "radical.ensemblemd.kernel_plugins.misc.ccount",
    "radical.ensemblemd.kernel_plugins.misc.chksum"
]
