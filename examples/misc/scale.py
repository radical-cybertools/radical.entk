#!/usr/bin/env python

import sys
import time

from radical.entk import Pipeline, Stage, Task, AppManager

n_pipes  = int(sys.argv[1])
n_stages = int(sys.argv[2])
n_tasks  = int(sys.argv[3])


# ------------------------------------------------------------------------------
#
def main():

    start = time.time()


    pipelines = set()
    for _ in range(n_pipes):

        p = Pipeline()

        for _ in range(n_stages):

            s = Stage()

            for _ in range(n_tasks):

                t = Task()
                t.executable = '/bin/true'

                s.add_tasks(t)

            p.add_stages(s)

        pipelines.add(p)


    appman = AppManager(autoterminate=True, rts='mock')
    appman.resource_desc = {'resource': 'local.localhost_test',
                            'cpus'    :  8,
                            'walltime': 10}
    appman.workflow      = pipelines

    appman.run()
    appman.terminate()

    print('%10.1f' % (time.time() - start))


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

  # import yappi
  # yappi.start(builtins=False)

    main()

  # yappi.get_thread_stats().print_all()
  # stats = yappi.convert2pstats(yappi.get_func_stats())
  # stats.dump_stats('pstats.prof')
  #
  # stats.print_stats()


# ------------------------------------------------------------------------------

