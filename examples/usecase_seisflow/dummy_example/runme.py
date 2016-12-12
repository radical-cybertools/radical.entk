__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.entk import EoP, AppManager, Kernel, ResourceHandle

from sims import sims_kernel
ENSEMBLE_SIZE=4


class Test(EoP):

    def __init__(self, ensemble_size, pipeline_size):
        super(Test,self).__init__(ensemble_size, pipeline_size)

    def stage_1(self, instance):
        global ENSEMBLE_SIZE

        k1 = Kernel(name="sims")
        k1.arguments = []
        k1.copy_input_data = [  '$SHARED/CMTSOLUTION',
                                '$SHARED/STATIONS',
                                '$SHARED/specfem_mockup',
                                '$SHARED/Par_file',
                                '$SHARED/addressing.txt',
                                '$SHARED/values_from_mesher.h'

                            ]
        k1.cores = 2
        k1.mpi=True

        return k1

   

if __name__ == '__main__':

    # Create an application manager
    app = AppManager(name='seisflow')

    # Register kernels to be used
    app.register_kernels(sims_kernel)

    # Create a resource handle for target machine
    res = ResourceHandle(resource="local.localhost",
                cores=4,
                #username='vivek91',
                #project = 'TG-MCB090174',
                #queue='development',
                walltime=10,
                database_url='mongodb://rp:rp@ds015335.mlab.com:15335/rp')

    res.shared_data = [ './input_data/CMTSOLUTION',
                        './input_data/STATIONS',
                        './input_data/bin/specfem_mockup',
                        './input_data/Par_file',
                        './input_data/addressing.txt',
                        './input_data/values_from_mesher.h']

    try:

        # Submit request for resources + wait till job becomes Active
        res.allocate(wait=True)

        # Create pattern object with desired ensemble size, pipeline size
        pipe = Test(ensemble_size=ENSEMBLE_SIZE, pipeline_size=1)

        # Add workload to the application manager
        app.add_workload(pipe)

        # Run the given workload
        res.run(app)

    except Exception, ex:
        print 'Application failed, error: ', ex


    finally:
        # Deallocate the resource
        res.deallocate()
    
