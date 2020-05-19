#!/usr/bin/env python
__copyright__ = 'Copyright 2013-2018, http://radical.rutgers.edu'
__license__ = 'MIT'


import os
import sys
import glob
import pprint
import radical.utils as ru
import radical.entk as re
import radical.analytics as ra

"""This example illustrates hoq to obtain durations for arbitrary (non-state)
profile events. Modified from examples under RADICAL Analytics"""

# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    loc = './re.session.two.vivek.017759.0012'
    src = os.path.dirname(loc)
    sid = os.path.basename(loc)
    session = ra.Session(src=src, sid = sid, stype='radical.entk')

    # A formatting helper before starting...
    def ppheader(message):
        separator = '\n' + 78 * '-' + '\n'
        print(separator + message + separator)

    # First we look at the *event* model of our session.  The event model is
    # usually less stringent than the state model: not all events will always be
    # available, events may have certain fields missing, they may be recorded
    # multiple times, their meaning may slightly differ, depending on the taken
    # code path.  But in general, these are the events available, and their
    # relative ordering.
    ppheader("event models")
    pprint.pprint(session.describe('event_model'))
    pprint.pprint(session.describe('statistics'))

    # Let's say that we want to see how long EnTK took to schedule, execute, and
    # process completed tasks.

    # We first filter our session to obtain only the task objects
    tasks = session.filter(etype='task', inplace=False)
    print('#tasks   : %d' % len(tasks.get()))

    # We use the 're.states.SCHEDULING' and 're.states.SUBMITTING' probes to find
    # the time taken by EnTK to create and submit all tasks for execution
    ppheader("Time spent to create and submit the tasks")
    duration = tasks.duration(event=[{ru.EVENT: 'state',
                                    ru.STATE: re.states.SCHEDULING},
                                    {ru.EVENT: 'state',
                                    ru.STATE: re.states.SUBMITTING}])
    print('duration : %.2f' % duration)

    # We use the 're.states.SUBMITTING' and 're.states.COMPLETED' probes to find
    # the time taken by EnTK to execute all tasks
    ppheader("Time spent to execute the tasks")
    duration = tasks.duration(event=[{ru.EVENT: 'state',
                                    ru.STATE: re.states.SUBMITTING},
                                    {ru.EVENT: 'state',
                                    ru.STATE: re.states.COMPLETED}])
    print('duration : %.2f' % duration)

    # We use the 're.states.COMPLETED' and 're.states.DONE' probes to find
    # the time taken by EnTK to process all executed tasks
    ppheader("Time spent to process executed tasks")
    duration = tasks.duration(event=[{ru.EVENT: 'state',
                                    ru.STATE: re.states.COMPLETED},
                                    {ru.EVENT: 'state',
                                    ru.STATE: re.states.DONE}])
    print('duration : %.2f' % duration)

    # Finally, we produce a list of the number of concurrent tasks between
    # states 're.states.SUBMITTING' and 're.states.COMPLETED' over the course
    # of the entire execution sampled every 10 seconds
    ppheader("concurrent tasks in between SUBMITTING and EXECUTED states")
    concurrency = tasks.concurrency(event=[{ru.EVENT: 'state',
                                            ru.STATE: re.states.SUBMITTING},
                                            {ru.EVENT: 'state',
                                            ru.STATE: re.states.COMPLETED}],
                                    sampling=10)
    pprint.pprint(concurrency)


# ------------------------------------------------------------------------------
