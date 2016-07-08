#!/usr/bin/env python

"""List of all registered kernels.

This registry is used to locate and load kernels. The entries must be
formatted in dotted python module notation.
"""

__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"


kernel_registry = [
    "radical.ensemblemd.kernel_plugins.md.pre_coam_loop",
    "radical.ensemblemd.kernel_plugins.md.amber",
    "radical.ensemblemd.kernel_plugins.md.coco",
    "radical.ensemblemd.kernel_plugins.md.tleap",

    "radical.ensemblemd.kernel_plugins.md.mmpbsa",
    "radical.ensemblemd.kernel_plugins.md.namd",
    "radical.ensemblemd.kernel_plugins.md.re_exchange",

    "radical.ensemblemd.kernel_plugins.md.pre_grlsd_loop",
    "radical.ensemblemd.kernel_plugins.md.gromacs",
    "radical.ensemblemd.kernel_plugins.md.pre_lsdmap",
    "radical.ensemblemd.kernel_plugins.md.lsdmap",
    "radical.ensemblemd.kernel_plugins.md.post_lsdmap",

    "radical.ensemblemd.kernel_plugins.misc.nop",
    "radical.ensemblemd.kernel_plugins.misc.idle",
    "radical.ensemblemd.kernel_plugins.misc.mkfile",
    "radical.ensemblemd.kernel_plugins.misc.hello",
    "radical.ensemblemd.kernel_plugins.misc.cat",
    "radical.ensemblemd.kernel_plugins.misc.ccount",
    "radical.ensemblemd.kernel_plugins.misc.chksum",
    "radical.ensemblemd.kernel_plugins.misc.levenshtein",
    "radical.ensemblemd.kernel_plugins.misc.diff",
    "radical.ensemblemd.kernel_plugins.misc.randval",
    "radical.ensemblemd.kernel_plugins.misc.randval_2"
]
