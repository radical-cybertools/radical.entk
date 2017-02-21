__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.entk import EoP, AppManager, Kernel, ResourceHandle

from echo import echo_kernel
from randval import rand_kernel
from sleep import sleep_kernel

ENSEMBLE_SIZE=4
ITER = [1 for x in range(1, ENSEMBLE_SIZE+1)]

class Test(EoP):

    def __init__(self, ensemble_size, pipeline_size,name):
        super(Test,self).__init__(ensemble_size, pipeline_size,name=name)

    def stage_1(self, instance):
        global ENSEMBLE_SIZE

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
        global ENSEMBLE_SIZE

        if ITER[instance-1] != 2:
            ITER[instance-1]+=1
            self.set_next_stage(stage=1)
        else:            
            pass
    

if __name__ == '__main__':

    # Create an application manager
    app = AppManager(name='MSM')

    # Register kernels to be used
    app.register_kernels(echo_kernel)
    app.register_kernels(sleep_kernel)    

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


    #while (len(ITER)<8):

    # Create pattern object with desired ensemble size, pipeline size
    pipe = Test(ensemble_size=ENSEMBLE_SIZE, pipeline_size=2, name='pat_{0}'.format(len(ITER)))

    # Add workload to the application manager
    app.add_workload(pipe)

    # Run the given workload
    res.run(app)

    ENSEMBLE_SIZE += 2 
    ITER = [1 for x in range(1, ENSEMBLE_SIZE+1)]

    # Create pattern object with desired ensemble size, pipeline size
    pipe = Test(ensemble_size=ENSEMBLE_SIZE, pipeline_size=2, name='pat_{0}'.format(len(ITER)))

    # Add workload to the application manager
    app.add_workload(pipe)

    # Run the given workload
    res.run(app)

    # Deallocate the resource
    res.deallocate()
    
