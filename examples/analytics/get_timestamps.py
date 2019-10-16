#!/usr/bin/env python

import os
import pprint
import radical.utils as ru
import radical.entk as re
import radical.analytics as ra

__copyright__ = 'Copyright 2013-2018, http://radical.rutgers.edu'
__license__ = 'MIT'

"""
This example illustrates the use of the method ra.Session.get().
Modified from examples under RADICAL Analytics
"""

# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    loc = './re.session.two.vivek.017759.0012'
    src = os.path.dirname(loc)
    sid = os.path.basename(loc)
    session = ra.Session(src=src, sid=sid, stype='radical.entk')

    # A formatting helper before starting...
    def ppheader(message):
        separator = '\n' + 78 * '-' + '\n'
        print(separator + message + separator)

    # and here we go. As seen in example 01, we use ra.Session.list() to get the
    # name of all the types of entity of the session.
    etypes = session.list('etype')
    pprint.pprint(etypes)

    # We limit ourselves to the type 'task'. We use the method
    # ra.Session.get() to get all the objects in our session with etype 'task':
    ppheader("properties of the entities with etype 'task'")
    tasks = session.get(etype='task')
    pprint.pprint(tasks)


    # Mmmm, still a bit too many entities. We limit our analysis to a single
    # task. We use ra.Session.get() to select all the objects in the
    # session with etype 'task' and uid 'task.0000' and return them into a
    # list:
    ppheader("properties of the entities with etype 'task' and uid 'task.0000'")
    task = session.get(etype='task', uid='task.0000')
    pprint.pprint(task)


    # We may want also to look into the states of this task:
    ppheader("states of the entities with uid 'task.0000'")
    states = task[0].states
    pprint.pprint(states)

    # and extract the state we need. For example, the state 'SCHEDULED', that
    # indicates that the task has been scheduled. To refer to the state 'SCHEDULED',
    # and to all the other states of RADICAL-Pilot, we use the re.states.SCHEDULED property
    # that guarantees type checking.
    ppheader("Properties of the state re.SCHEDULED of the entities with uid 'task.0000'")
    state = task[0].states[re.states.SCHEDULED]
    pprint.pprint(state)

    # Finally, we extract a property we need from this state. For example, the
    # timestamp of when the task has been created, i.e., the property 'time' of
    # the state SCHEDULED:
    ppheader("Property 'time' of the state re.states.SCHEDULED of the entities with uid 'task.000000'")
    timestamp = task[0].states[re.states.SCHEDULED][ru.TIME]
    pprint.pprint(timestamp)

    # ra.Session.get() can also been used to to get all the entities in our
    # session that have a specific state. For example, the following gets all
    # the types of entity that have the state 'SCHEDULED':
    ppheader("Entities with state re.states.SCHEDULED")
    entities = session.get(state=re.states.SCHEDULED)
    pprint.pprint(entities)

    # We can then print the timestamp of the state 'SCHEDULED' for all the entities
    # having that state by using something like:
    ppheader("Timestamp of all the entities with state re.states.SCHEDULED")
    timestamps = [entity.states[re.states.SCHEDULED][ru.TIME] for entity in entities]
    pprint.pprint(timestamps)

    # We can also create tailored data structures for our analyis. For
    # example, using tuples to name entities, state, and timestamp:
    ppheader("Named entities with state re.states.SCHEDULED and its timestamp")
    named_timestamps = [(entity.uid,
                        entity.states[re.states.SCHEDULED][ru.STATE],
                        entity.states[re.states.SCHEDULED][ru.TIME]) for entity in entities]
    pprint.pprint(named_timestamps)
