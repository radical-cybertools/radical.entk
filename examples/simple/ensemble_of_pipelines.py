#!/usr/bin/env python

__author__       = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__    = "Copyright 2014, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "Pipeline Example (generic)"

import sys
import os
import json

from radical.entk import AppManager, Kernel, ResourceHandle, EoP, EnTKError


from kernel_defs.ccount import ccount_kernel
from kernel_defs.chksum import chksum_kernel
from kernel_defs.mkfile import mkfile_kernel


# ------------------------------------------------------------------------------
# Set default verbosity

if os.environ.get('RADICAL_ENTK_VERBOSE') == None:
    os.environ['RADICAL_ENTK_VERBOSE'] = 'REPORT'


# ------------------------------------------------------------------------------
#
class CharCount(EoP):
    """The CharCount class implements a three-stage pipeline. It inherits from
        radical.ensemblemd.Pipeline, the abstract base class for all pipelines.
    """

    def __init__(self, stages,instances):
        EoP.__init__(self, stages,instances)

    def stage_1(self, instance):
        """The first stage of the pipeline creates a 1 MB ASCI file.
        """
        k = Kernel(name="mkfile")
        k.arguments = ["--size=1000000", "--filename=asciifile-{0}.dat".format(instance)]
        return k

    def stage_2(self, instance):
        """The second stage of the pipeline does a character frequency analysis
           on the file generated the first stage. The result is transferred back
           to the host running this script.

           ..note:: The placeholder ``$STAGE_1`` used in ``link_input_data`` is
                    a reference to the working directory of stage 1. ``$STAGE_``
                    can be used analogous to refernce other stages.
        """
        k = Kernel(name="ccount")
        k.arguments            = ["--inputfile=asciifile-{0}.dat".format(instance), "--outputfile=cfreqs-{0}.dat".format(instance)]
        k.link_input_data      = "$STAGE_1/asciifile-{0}.dat".format(instance)
        k.download_output_data = "cfreqs-{0}.dat".format(instance)
        return k

    def stage_3(self, instance):
        """The third stage of the pipeline creates a checksum of the output file
           of the second stage. The result is transferred back to the host
           running this script.
        """
        k = Kernel(name="chksum")
        k.arguments            = ["--inputfile=cfreqs-{0}.dat".format(instance), "--outputfile=cfreqs-{0}.sha1".format(instance)]
        k.link_input_data      = "$STAGE_2/cfreqs-{0}.dat".format(instance)
        k.download_output_data = "cfreqs-{0}.sha1".format(instance)
        return k

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":


    # use the resource specified as argument, fall back to localhost
    if   len(sys.argv)  > 2: 
        print 'Usage:\t%s [resource]\n\n' % sys.argv[0]
        sys.exit(1)
    elif len(sys.argv) == 2: 
        resource = sys.argv[1]
    else: 
        resource = 'local.localhost'

    try:

        with open('%s/config.json'%os.path.dirname(os.path.abspath(__file__))) as data_file:    
            config = json.load(data_file)


        # Create an application manager
        app = AppManager(name='example_1')

        # Register kernels to be used
        app.register_kernels(chksum_kernel)
        app.register_kernels(ccount_kernel)
        app.register_kernels(mkfile_kernel)

        # Create a new resource handle with one resource and a fixed
        # number of cores and runtime.
        cluster = ResourceHandle(
                resource=resource,
                cores=config[resource]["cores"],
                walltime=15,
                #username=None,

                project=config[resource]['project'],
                access_schema = config[resource]['schema'],
                queue = config[resource]['queue'],
                database_url='mongodb://rp:rp@ds015335.mlab.com:15335/rp',
            )

        # Allocate the resources. 
        cluster.allocate()

        # Set the 'instances' of the pipeline to 16. This means that 16 instances
        # of each pipeline stage are executed.
        #
        # Execution of the 16 pipeline instances can happen concurrently or
        # sequentially, depending on the resources (cores) available in the
        # SingleClusterEnvironment.
        ccount = CharCount(pipeline_size=3, ensemble_size=16)

        # Add workload to the application manager
        app.add_workload(ccount)

        cluster.run(app)

    except EnTKError, er:

        print "Ensemble MD Toolkit Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace

    try:
        cluster.deallocate()
    except:
        pass