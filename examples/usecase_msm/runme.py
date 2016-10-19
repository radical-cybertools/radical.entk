__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.entk import EoP, AppManager, Kernel, ResourceHandle

from echo import echo_kernel
from randval import rand_kernel
from sleep import sleep_kernel

ENSEMBLE_SIZE=4

ITER = [1 for x in range(1, ENSEMBLE_SIZE+1)]
ITER2 = []

class Test(EoP):

    def __init__(self, ensemble_size, pipeline_size):
        super(Test,self).__init__(ensemble_size, pipeline_size)

    def stage_1(self, instance):
        k1 = Kernel(name="sleep")
        print 'I am task {0}/{1}'.format(instance, ENSEMBLE_SIZE)
        k1.arguments = ["--file=output.txt","--text={0}".format(instance),"--duration=10"]
        k1.cores = 1

        return k1


    def stage_2(self, instance):

        k2 = Kernel(name="echo")
        k2.arguments = ["--file=output.txt","--text=analysis"]
        k2.cores = 1

        return k2

    def branch_2(self, instance):

        global ITER        
        global ITER2

        if (ITER[instance-1] != 2):
            ITER[instance-1]+=1
            self.set_next_stage(stage=1)
        elif (ITER[0] == 2):
            self.set_ensemble_size(size=8)
            ENSEMBLE_SIZE=8
            ITER[0]+=1
            ITER2 = [1 for x in range(1,ENSEMBLE_SIZE+1)]
        else:
            pass
    

if __name__ == '__main__':

    # Create pattern object with desired ensemble size, pipeline size
    pipe = Test(ensemble_size=ENSEMBLE_SIZE, pipeline_size=2)

    # Create an application manager
    app = AppManager(name='MSM')

    # Register kernels to be used
    app.register_kernels(echo_kernel)
    app.register_kernels(sleep_kernel)

    # Add workload to the application manager
    app.add_workload(pipe)
    

    # Create a resource handle for target machine
    res = ResourceHandle(resource="local.localhost",
                cores=4,
                #username='vivek91',
                #project = 'TG-MCB090174',
                #queue='development',
                walltime=10,
                database_url='mongodb://rp:rp@ds015335.mlab.com:15335/rp')

    # Submit request for resources + wait till job becomes Active
    res.allocate(wait=True)

    # Run the given workload
    res.run(app)

    # Deallocate the resource
    res.deallocate()
    