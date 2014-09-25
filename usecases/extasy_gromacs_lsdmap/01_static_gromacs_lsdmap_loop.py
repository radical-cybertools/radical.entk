#!/usr/bin/env python
"""
TODO Vivek: Add description and instructions how to run, where to get
sample data from, etc. Refer to other use-cases for 'inspiration'.

Run this example with ``RADICAL_ENMD_VERBOSE`` set to ``info`` if you want to
see log messages about plug-in invocation and simulation progress::

    RADICAL_ENMD_VERBOSE=info python 01_static_gromacs_lsdmap_loop.py
"""

__author__        = "Vivek <vivek.balasubramanian@rutgers.edu>"
__copyright__     = "Copyright 2014, http://radical.rutgers.edu"
__license__       = "MIT"
__use_case_name__ = "'Gromacs + LSDMap' simulation-analysis proof-of-concept (ExTASY)."
  
  
from radical.ensemblemd import Kernel
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import SingleClusterEnvironment


# ------------------------------------------------------------------------------
#
class Gromacs_LSDMap(SimulationAnalysisLoop):
  # TODO Vivek: add description.

    def __init__(self, maxiterations, simulation_instances=1, analysis_instances=1):
        SimulationAnalysisLoop.__init__(self, maxiterations, simulation_instances, analysis_instances)
    

    def simulation_step(self, iteration, instance):
        '''TODO Vivek: add description of this step.
        '''
        pre_sim = Kernel(name="md.pre_gromacs")
        pre_sim.upload_input_data = ["input%s.gro" %(iteration),"file_splitter.sh"]
        pre_sim.arguments = ["--inputfile=input%s.gro"%(iteration)]
        #pre_sim.download_output_data = ['input-iter%{0}-%{1}.gro'.format(iteration,instance)]

        gromacs = Kernel(name="md.gromacs")
        # k.set_upload_input(files=['run.sh','input-iter%{0}-%{1}.gro > start.gro'.format(iteration,instance),'grompp.mdp','topol.top'])
        gromacs.arguments = ["--grompp=grompp.mdp","--topol=topol.top","--inputfile=start.gro","--outputfile=out.gro"]
        gromacs.link_input_data = ['$STEP_1/input-iter%{0}-%{1}.gro > start.gro'.format(iteration,instance)]
        gromacs.copy_input_data = ['grompp.mdp','topol.top','run.sh']
        gromacs.download_output_data = ['out.gro > out-iter%{0}-%{1}.gro'.format(iteration, instance)]

        post_sim = Kernel(name="md.post_gromacs")
        post_sim.arguments = ["--outputfile=tmp.gro","--numCUs=64"]
        post_sim.copy_input_data = ['out-iter%{0}-%{1}.gro'.format(iteration,instance)]
        post_sim.download_output_data = ['tmp.gro > tmp%{0}.gro'.format(iteration)]

        return [pre_sim, gromacs, post_sim]
    
    def analysis_step(self, iteration, instance):
        '''TODO Vivek: add description of this step.
        '''
        lsdmap = Kernel(name="md.lsdmap")
        # k.set_upload_input(files=['config.ini','tmp%{0}.gro'.format(iteration),'run_analyzer.sh'])
        lsdmap.arguments = ["--nnfile=out.nn","--wfile=weight.w","--config=config.ini","--inputfile=tmp.gro"]
        lsdmap.copy_input_data = ['config.ini','tmp%{0}.gro > tmp.gro'.format(iteration),'run_analyzer.sh']
        lsdmap.download_output_data = ['tmp.eg > out.eg','tmp.ev > out.ev','out.nn','lsdmap.log']
    
        reweight = Kernel(name="md.update_reweight")
        # k.set_upload_input(files=['out.ev','out.nc','tmp%{0}.gro'.format(iteration)],'out.nn','weight.w')
        reweight.arguments = ["--nruns=10000","--evfile=out.ev","--clones=out.nc","--grofile=tmp.gro","--nnfile=out.nn","--wfile=weight.w","--outputfile=out.gro"]
        reweight.copy_input_data = ["out.ev","tmp%{0}.gro > tmp.gro".format(iteration),'out.nn','select.py','reweighting.py']
        reweight.download_output_data = ['out.gro > input%s.gro'%(iteration+1)]

        return [lsdmap, reweight]


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

  try:
      # Create a new static execution context with one resource and a fixed
      # number of cores and runtime.
      cluster = SingleClusterEnvironment(
        resource="localhost",
        cores=1,
        walltime=15
      )

      # We set the 'instances' of the simulation step to 16. This means that 16
      # instances of the simulation are executed every iteration.
      # We set the 'instances' of the analysis step to 1. This means that only
      # one instance of the analysis is executed for each iteration
      randomsa = Gromacs_LSDMap(maxiterations=64, simulation_instances=16, analysis_instances=1)

      cluster.run(randomsa)
  
  except EnsemblemdError, er:

    print "EnsembleMD Error: {0}".format(str(er))
    raise # Just raise the execption again to get the backtrace
