#!/usr/bin/env python3

import functools

import radical.entk as re


N_ENSEMBLES   =    5
ENSEMBLE_SIZE =    5
MAX_ITER      =   10
LIMIT         =  100 * 1000 * 1000

# we want to change pipelines on the fly, thus want to keep track
# of all pipelines
ensemble = dict()


# ------------------------------------------------------------------------------
#
def get_stage_1(sandbox, pname):

    t1 = re.Task()
    t1.executable = '/bin/sh'
    t1.arguments  = ['-c', 'od -An -N1 -i /dev/random']
    t1.stdout     = 'random.txt'
    t1.sandbox    = sandbox
    t1.download_output_data = ['random.txt > %s.random.txt' % pname]

    s1 = re.Stage()
    s1.add_tasks(t1)

    # --------------------------------------------------------------------------
    # use a callback after that stage completed for output of the seed value
    def post_exec(stage, pname):
        seed = int(open('%s.random.txt' % pname).read().split()[-1])
        print(pname, 'rand  --- - %10d' % seed)
    # --------------------------------------------------------------------------
    s1.post_exec = functools.partial(post_exec, s1, pname)

    return s1


# ------------------------------------------------------------------------------
#
def get_stage_2(sandbox, pname, iteration=0):

    # this is the pseudo model we iterate on the second stage
    model = 'echo "($(tail -n 1 random.txt) + %(iteration)d) ^ %(ensemble_id)d" | bc'

    # second stage: create ENSEMBLE_SIZE tasks to compute the n'th power
    s2 = re.Stage()
    s2.iteration = iteration
    for i in range(ENSEMBLE_SIZE):
        t2 = re.Task()
        t2.executable = '/bin/sh'
        t2.arguments  = ['-c', model % {'iteration'  : iteration,
                                        'ensemble_id': i}]
        t2.stdout     = 'power.%03d.txt' % i
        t2.sandbox    = sandbox
        t2.download_output_data = ['%s > %s.%s' % (t2.stdout, pname, t2.stdout)]
        s2.add_tasks(t2)

    # --------------------------------------------------------------------------
    # use a callback after that stage completed which checks the
    # intermediate value:
    #  - if value     > LIMIT   : new seed  (add stage 1)
    #  - if iteration > MAX_ITER: abort     (add stage 3)
    #  - else                   : continue  (add stage 2 again)
    def post_exec(stage, pname):

        pipeline = ensemble[pname]

        # continue to iterate - check intermediate data
        total = 0
        for task in stage.tasks:
            data   = open('%s.%s' % (pname, task.stdout)).read()
            total += int(data.split()[-1])

        if total > LIMIT:
            # re-seed the pipeline (inject a new stage 1)
            print(pipeline.name, 'seed  %3d - %10d' % (stage.iteration, total))
            pipeline.add_stages(get_stage_1(sandbox, pname))
            pipeline.add_stages(get_stage_2(sandbox, pname))

        elif stage.iteration > MAX_ITER:
            # iteration limit reached, discontinue pipeline (add final stage 3)
            print(pipeline.name, 'break %3d - %10d' % (stage.iteration, total))
            pipeline.add_stages(get_stage_3(sandbox, pname))

        else:
            # continue to iterate (increase the iteration counter)
            print(pipeline.name, 'iter  %3d - %10d' % (stage.iteration, total))
            pipeline.add_stages(get_stage_2(sandbox, pname, stage.iteration + 1))

    # --------------------------------------------------------------------------
    s2.post_exec = functools.partial(post_exec, s2, pname)

    return s2


# ------------------------------------------------------------------------------
#
def get_stage_3(sandbox, pname):

    # third stage: compute sum over all powers
    t3 = re.Task()
    t3.executable = '/bin/sh'
    t3.arguments  = ['-c', 'cat power.*.txt | paste -sd+ | bc']
    t3.stdout     = 'sum.txt'
    t3.sandbox    = sandbox
    t3.download_output_data = ['%s > %s.%s' % (t3.stdout, pname, t3.stdout)]

    # download the result while renaming to get unique files per pipeline
    t3.download_output_data = ['sum.txt > %s.sum.txt' % pname]

    s3 = re.Stage()
    s3.add_tasks(t3)

    # --------------------------------------------------------------------------
    # use a callback after that stage completed for output of the final result
    def post_exec(stage, pname):
        total = int(open('%s.sum.txt' % pname).read())
        print(pname, 'final %3d - %10d' % (MAX_ITER, total))
    # --------------------------------------------------------------------------
    s3.post_exec = functools.partial(post_exec, s3, pname)

    return s3


# ------------------------------------------------------------------------------
#
def generate_pipeline(pname):
    '''
    We generate essentially the same pipeline as in `radical_entk_1.py` with
    three stages:

      1) generate a random seed as input data
      2) evolve a model based on that input data via a set of ensembles
      3) derive a common metric across the model results

    However, we will iterate the model (stage 2) multiple times and check for
    intermediate results.  Further, we will cancel all pipelines whose
    intermediate result is larger than some threshold and will instead replace
    that pipeline with a newly seeded pipeline.  We break after a certain number
    of iterations and expect the result to be biased toward smaller seeds.
    '''

    # all tasks in this pipeline share the same sandbox
    sandbox = pname

    # first stage: create 1 task to generate a random seed number
    s1 = get_stage_1(sandbox, pname)

    # second stage: create ENSEMBLE_SIZE tasks to compute the n'th power
    # of that number (this stage runs at least once)
    s2 = get_stage_2(sandbox, pname)

    # the third stage is added dynamically after convergence, so we return
    # the pipeline with the initial two stages
    p = re.Pipeline()
    p.name = pname
    p.add_stages(s1)
    p.add_stages(s2)

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
        'walltime': 10,
        'cpus'    : 1
    }

    # create an ensemble of n simulation pipelines
    for cnt in range(N_ENSEMBLES):
        pname = 'pipe.%03d' % cnt
        ensemble[pname] = generate_pipeline(pname)

    # assign the workflow to the application manager, then
    # run the ensemble and wait for completion
    appman.workflow = set(ensemble.values())
    appman.run()

    # check results which were staged back
    for cnt in range(N_ENSEMBLES):
        result = int(open('pipe.%03d.sum.txt' % cnt).read())
        print('%18d - %10d' % (cnt, result))


# ------------------------------------------------------------------------------

