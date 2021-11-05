#!/usr/bin/env python3

import radical.entk as re


# ------------------------------------------------------------------------------
#
def generate_pipeline(uid):
    '''
    Generate a (fake) simulation pipeline.  We assume a simple pipeline
    structure consisting of three steps:

      1) generate a random seed as input data
      2) evolve a model based on that input data via a set of ensembles
      3) derive a common metric across the model results

    Similar patterns are frequently found in MD simulation workflows.  For the
    purpose of this tutorial, the `random seed` is a random  integer between
    0 and 255, the model is to compute the n'th power of that integer, and the
    common metric is the sum of those powers.
    '''

    # all tasks in this pipeline share the same sandbox
    sandbox = uid

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
    for i in range(10):
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

    # download the result while renaming to get unique files per pipeline
    t3.download_output_data = ['sum.txt > %s.sum.txt' % uid]

    s3 = re.Stage()
    s3.add_tasks(t3)

    # assemble the three stages into a pipeline
    p = re.Pipeline()
    p.add_stages(s1)
    p.add_stages(s2)
    p.add_stages(s3)

    return p


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # create application manager which executes our ensemble
    appman = re.AppManager()

    # assign resource request description to the application manager using
    # three mandatory keys: target resource, walltime, and number of cpus
    appman.resource_desc = {
        'resource': 'local.localhost_flux',
        'walltime': 10,
        'cpus'    : 1
    }

    # create an ensemble of n simulation pipelines
    n_ensembles = 10
    ensemble = set()
    for cnt in range(n_ensembles):
        ensemble.add(generate_pipeline(uid='pipe.%03d' % cnt))

    # assign the workflow to the application manager, then
    # run the ensemble and wait for completion
    appman.workflow = ensemble
    appman.run()

    # check results which were staged back
    for cnt in range(n_ensembles):
        data = open('pipe.%03d.sum.txt' % cnt).read()
        try:
            result = int(data)
        except:
            print('==%s==' % str(data))
            raise
        print('%3d -- %25d' % (cnt, result))


# ------------------------------------------------------------------------------

