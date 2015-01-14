#!/usr/bin/env python

""" 
Extasy project: 'Coco/Amber' STATIC simulation-analysis loop proof-of-concept (Nottingham).
"""

__author__        = "Vivek <vivek.balasubramanian@rutgers.edu>"
__copyright__     = "Copyright 2014, http://radical.rutgers.edu"
__license__       = "MIT"
__use_case_name__ = "'Amber + CoCo' simulation-analysis proof-of-concept (ExTASY)."


from radical.ensemblemd import Kernel
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import SingleClusterEnvironment


# ------------------------------------------------------------------------------
num_sims=8
nsave = 2
#
class Extasy_CocoAmber_Static(SimulationAnalysisLoop):

    def __init__(self, maxiterations, simulation_instances, analysis_instances):
        SimulationAnalysisLoop.__init__(self, maxiterations, simulation_instances, analysis_instances)

    def pre_loop(self):
        '''
        function : transfers input files, intermediate executables
        '''
        k = Kernel(name="md.pre_coam_loop")
        k.upload_input_data = ['penta.crd','mdshort.in','min.in','penta.top','postexec.py']
        return k


    def simulation_step(self, iteration, instance):
        '''
        function : if iteration = 1, use .crd file from pre_loop, else use .crd output from analysis in current
        iteration. Perform amber on the .crd files to generate a set of .ncdf files.
        '''
        k = Kernel(name="md.amber")
        k.arguments = ["--mininfile=min.in","--mdinfile=mdshort.in","--topfile=penta.top","--cycle=%s"%(iteration)]
        k.link_input_data = ['$PRE_LOOP/min.in','$PRE_LOOP/penta.top','$PRE_LOOP/mdshort.in']
        if((iteration-1)==0):
            k.link_input_data = k.link_input_data + ['$PRE_LOOP/penta.crd > min1.crd']
        else:
            k.link_input_data = k.link_input_data + ['$PREV_ANALYSIS_INSTANCE_1/min{0}{1}.crd > min{2}.crd'.format(iteration-1,instance-1,iteration)]
        if(iteration%nsave==0):
            k.download_output_data = ['md{0}.ncdf > backup/iter{0}/md_{0}_{1}.ncdf'.format(iteration,instance)]
        return k


    def analysis_step(self, iteration, instance):
        '''
        function : if iteration = 1, pass, else, perform CoCo Analysis on the output of the simulation from the
        previous iteration. Generate the .crd file to be used in the next simulation in the current iteration.
        '''
        k = Kernel(name="md.coco")
        k.arguments = ["--grid=5","--dims=3","--frontpoints=%s"%num_sims,"--topfile=penta.top","--mdfile=*.ncdf","--output=pentaopt%s"%(iteration),"--cycle=%s"%(iteration)]
        k.link_input_data = ['$PRE_LOOP/penta.top','$PRE_LOOP/postexec.py']
        k.cores = 16
        for iter in range(1,iteration+1):
            for i in range(1,num_sims+1):
                k.link_input_data = k.link_input_data + ['$SIMULATION_ITERATION_{0}_INSTANCE_{1}/md{0}.ncdf > md_{0}_{1}.ncdf'.format(iter,i)]
        return k

    def post_loop(self):
        pass


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.
        cluster = SingleClusterEnvironment(
            resource="stampede.tacc.utexas.edu",
            cores=16,
            walltime=30,
            username = 'vivek91',
            allocation = 'TG-MCB090174'
        )

        coco_amber_static = Extasy_CocoAmber_Static(maxiterations=4, simulation_instances=num_sims, analysis_instances=1)
        cluster.run(coco_amber_static)

    except EnsemblemdError, er:

        print "EnsembleMD Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
