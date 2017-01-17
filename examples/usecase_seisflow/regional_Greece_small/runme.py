__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.entk import EoP, AppManager, Kernel, ResourceHandle, PoE
import argparse
from meshfem import meshfem_kernel
from specfem import specfem_kernel
ENSEMBLE_SIZE=4


class Test(PoE):

    def __init__(self, ensemble_size, pipeline_size):
        super(Test,self).__init__(ensemble_size, pipeline_size)

    def stage_1(self, instance):
        global ENSEMBLE_SIZE

        k1 = Kernel(name="meshfem")
        k1.arguments = []
        k1.copy_input_data = [  '$SHARED/ipdata.tar']
        k1.copy_output_data = ['opdata.tar > $SHARED/opdata.tar']
        k1.cores = 4
        k1.mpi = True

        return k1

    def stage_2(self, instance):

        k1 = Kernel(name="specfem")
        k1.arguments = []
        k1.copy_input_data = ['$SHARED/opdata.tar > opdata.tar']
        k1.cores = 8
        k1.mpi = True

        return k1        

   

if __name__ == '__main__':

    # Create an application manager
    app = AppManager(name='seisflow')

    # Register kernels to be used
    app.register_kernels(meshfem_kernel)
    app.register_kernels(specfem_kernel)

    parser = argparse.ArgumentParser()
    parser.add_argument('--resource', help='target resource label')
    args = parser.parse_args()
    
    if args.resource != None:
        resource = args.resource
    else:
        resource = 'local.localhost'



    res_dict = {
                    'xsede.stampede': { 'cores': '16', 
                                        # 'username': 'vivek91', 
                                        'username': 'tg838801', 
                                        'project': 'TG-MCB090174',
                                        'queue': 'development', 
                                        'walltime': '40', 
                                        'schema': 'gsissh'
                                    },

                    'local.localhost': {'cores': '4', 
                                        'username': None, 
                                        'project': None, 
                                        'queue': None, 
                                        'walltime': 40, 
                                        'schema': None
                                    }
            }

    # Create a resource handle for target machine
    res = ResourceHandle(resource=resource,
                cores=res_dict[resource]['cores'],
                username=res_dict[resource]['username'],
                project =res_dict[resource]['project'] ,
                queue=res_dict[resource]['queue'],
                walltime=res_dict[resource]['walltime'],
                database_url='mongodb://rp:rp@ds015335.mlab.com:15335/rp',
                access_schema = res_dict[resource]['schema']                
)

    res.shared_data = [ './input_data/ipdata.tar']

    try:

        # Submit request for resources + wait till job becomes Active
        res.allocate(wait=True)

        # Create pattern object with desired ensemble size, pipeline size
        pipe = Test(ensemble_size=[1,4], pipeline_size=2)

        # Add workload to the application manager
        app.add_workload(pipe)

        # Run the given workload
        res.run(app)

    except Exception, ex:
        print 'Application failed, error: ', ex


    finally:
        # Deallocate the resource
        res.deallocate()
    
