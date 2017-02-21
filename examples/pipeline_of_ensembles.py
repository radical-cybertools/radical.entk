#!/usr/bin/env python

__author__       = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__    = "Copyright 2016, http://radical.rutgers.edu"
__license__      = "MIT"
__example_name__ = "Pipeline of Ensembles Example (generic)"

import sys
import os
import json

from radical.entk import AppManager, Kernel, ResourceHandle, PoE

from kernel_defs.ccount import ccount_kernel
from kernel_defs.chksum import chksum_kernel

# ------------------------------------------------------------------------------
# Set default verbosity

if os.environ.get('RADICAL_ENTK_VERBOSE') == None:
    os.environ['RADICAL_ENTK_VERBOSE'] = 'INFO'


# ------------------------------------------------------------------------------
#
class Test(PoE):
    """The CalculateChecksums class implements a Bag of Tasks. 
    """

    def __init__(self, ensemble_size, pipeline_size):
        super(Test,self).__init__(ensemble_size, pipeline_size)

    def stage_1(self, instance):

        """
        This stage calculates the number of characters in a UTF file.
        """
        k = Kernel(name="ccount")
        k.arguments = ["--inputfile=UTF-8-demo.txt", "--outputfile=ccount-{0}.txt".format(instance)]
        k.upload_input_data  = "UTF-8-demo.txt"

        return k


    def stage_2(self, instance):

        """This stage calculates the SHA1 checksum of a UTF file. The checksum is written
           to an output file and tranferred back to the host running this
           script.
        """
        k = Kernel(name="chksum")
        k.arguments            = ["--inputfile=UTF-8-demo.txt", "--outputfile=checksum{0}.sha1".format(instance)]
        k.upload_input_data  = "UTF-8-demo.txt"
        k.download_output_data = "checksum{0}.sha1".format(instance)
        
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

        os.system('wget -q -o UTF-8-demo.txt http://www.cl.cam.ac.uk/~mgk25/ucs/examples/UTF-8-demo.txt')

        # Create an application manager
        app = AppManager(name='example_1')

        # Register kernels to be used
        app.register_kernels(checksum_kernel)
        app.register_kernels(ccount_kernel)

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
                database_url='mongodb://entk:entk@ds129459.mlab.com:29459/entk_doc',
            )

        # Allocate the resources.
        cluster.allocate(wait=True)

        # Create pattern object with desired ensemble size, pipeline size
        pipe = Test(ensemble_size=[2,4], pipeline_size=2)

        # Add workload to the application manager
        app.add_workload(pipe)

        # Run the given workload
        cluster.run(app)

    except Exception, ex:
        print 'Application failed, error: ', ex

    finally:
        # Deallocate the resource
        cluster.deallocate()