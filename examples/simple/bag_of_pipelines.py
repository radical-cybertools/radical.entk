#!/usr/bin/env python3

# pylint: disable=import-error

import sys
import time

import radical.entk as re

watch = list()


# ------------------------------------------------------------------------------
#
def timeit(msg):

    watch.append(time.time())

    if len(watch) > 1:
        print('%-11s: %6.2f' % (msg, watch[-1] - watch[-2]))
    else:
        print()


# ------------------------------------------------------------------------------
#
def main():

    timeit('start')

    n_pipes  = int(sys.argv[1])
    n_stages = int(sys.argv[2])
    n_tasks  = int(sys.argv[3])
    backend  = str(sys.argv[4])

    tot_tasks  = n_pipes * n_stages * n_tasks
    tot_stages = n_pipes * n_stages
    tot_nodes  = tot_tasks + tot_stages + n_pipes + 1

    print('%s: %d x %d x %d = %d tasks / %d nodes'
         % (backend, n_pipes, n_stages, n_tasks, tot_tasks, tot_nodes))

    td = {'executable': 'true'}

    am = re.AppManager(rts=backend, autoterminate=True)

    pipelines = list()
    for _ in range(n_pipes):

        pipeline = re.Pipeline()
        pipelines.append(pipeline)

        for _ in range(n_stages):

            stage = re.Stage()
            pipeline.add_stages([stage])

            for _ in range(n_tasks):

                task = re.Task(td)
                stage.add_tasks([task])

    am.workflow = set(pipelines)
    timeit('create')

    rd = {'resource' : 'local.localhost',
          'cpus'     : 32,
          'walltime' : 15}
    am.resource_desc = rd
    timeit('backend')

  # rw.puml_plot([wf], depth=2)
  # timeit('plot (puml)')

    timeit('run')
    am.run()
    timeit('wait')
    am.terminate()
    timeit('close')


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    use_yappi = False

    if use_yappi:
        import yappi
        yappi.set_clock_type("wall")
        yappi.start(builtins=True)

    main()

    if use_yappi:
        import yappi
        yappi.get_thread_stats().print_all()
        stats = yappi.convert2pstats(yappi.get_func_stats())
        stats.dump_stats('pstats.prof')


# ------------------------------------------------------------------------------

