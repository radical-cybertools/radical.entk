#!/usr/bin/env python3

import radical.entk as re

#
# This example demonstrates the execution of a simple ensemble of simulations.
# Each ensemble member is in itself a pipeline of three different stages:
#
#     1) generate a random seed as input data
#     2) evolve a model based on that input data via a set of ensembles
#     3) derive a common metric across the model results
#
#   Similar patterns are frequently found in MD simulation workflows.  For the
#   purpose of this tutorial, the stages are:
#
#   - random seed  : create a random number)
#   - evolve model : N tasks computing n'th power of the input)
#   - common metric: sum over all 'model' outputs
#
# The final results are then staged back and printed on STDOUT.
#


# ------------------------------------------------------------------------------
#
def generate_pipeline(pname):
    '''
    Generate a single simulation pipeline, i.e., a new ensemble member.
    The pipeline structure consisting of three steps as described above.
    '''

    # all tasks in this pipeline share the same sandbox
    sandbox = pname

    # first stage: create 1 task to generate a random seed number
    t1 = re.Task()
    t1.executable = '/bin/sh'
    t1.arguments  = ['-c', 'od -An -N1 -i /dev/random']
    t1.stdout     = 'random.txt'
    t1.sandbox    = sandbox

    s1 = re.Stage()
    s1.add_tasks(t1)

    # second stage: create 10 tasks to compute the n'th power of that number
    s2 = re.Stage()
    n_simulations = 10
    for i in range(n_simulations):
        t2 = re.Task()
        t2.executable = '/bin/sh'
        t2.arguments  = ['-c', 'echo "$(cat random.txt) ^ %d" | bc' % i]
        t2.stdout     = 'power.%03d.txt' % i
        t2.sandbox    = sandbox
        s2.add_tasks(t2)

    # third stage: compute sum over all powers
    t3 = re.Task()
    t3.executable = '/bin/sh'
    t3.arguments  = ['-c', 'cat power.*.txt | paste -sd+ | bc']
    t3.stdout     = 'sum.txt'
    t3.sandbox    = sandbox

    s3 = re.Stage()
    s3.add_tasks(t3)

    # fourth stage: compute square root of previous sum
    t4 = re.Task()
    t4.executable = '/bin/sh'
    t4.arguments  = ['-c', 'echo "sqrt($(cat sum.txt))" | bc']
    t4.stdout     = 'sqrt.txt'
    t4.sandbox    = sandbox

    # download the result while renaming to get unique files per pipeline
    t4.download_output_data = ['sqrt.txt > %s.sqrt.txt' % pname]

    s4 = re.Stage()
    s4.add_tasks(t4)

    # assemble the three stages into a pipeline and return it
    p = re.Pipeline()
    p.add_stages(s1)
    p.add_stages(s2)
    p.add_stages(s3)
    p.add_stages(s4)

    return p


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # create application manager which executes our ensemble
    appman = re.AppManager()

    # assign resource request description to the application manager using
    # three mandatory keys: target resource, walltime, and number of cpus
    appman.resource_desc = {
        'resource': 'local.localhost',
      # 'resource': 'local.localhost_flux',
        'walltime': 10,
        'cpus'    : 1
    }

    # create an ensemble of n simulation pipelines
    n_pipelines = 10
    ensemble = set()
    for cnt in range(n_pipelines):
        ensemble.add(generate_pipeline(pname='pipe.%03d' % cnt))

    # assign the workflow to the application manager, then
    # run the ensemble and wait for completion
    appman.workflow = ensemble
    appman.run()

    # check results which were staged back
    for cnt in range(n_pipelines):
        pipe_fname  = 'pipe.%03d.sqrt.txt' % cnt
        pipe_result = float(open(pipe_fname, encoding='utf-8').read())
        print('%3d -- %25.2f' % (cnt, pipe_result))


# ------------------------------------------------------------------------------

