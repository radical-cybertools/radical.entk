import radical.analytics as ra
import radical.entk as re
import pprint

# A formatting helper before starting...
def ppheader(message):
    separator = '\n' + 78 * '-' + '\n'
    print separator + message + separator

if __name__ == '__main__':

    session = ra.Session(stype='radical.entk',src='/home/vivek/Research/repos/radical.entk-0.6/examples/analytics/raw_data/')

    # A formatting helper before starting...
    def ppheader(message):
        separator = '\n' + 78 * '-' + '\n'
        print separator + message + separator

    # and here we go. Once we filter our session object so to keep only the
    # relevent entities (as seen in example 03), we are ready to perform our
    # analyses :) Currently, RADICAL-Analytics supports two types of analysis:
    # duration and concurrency. This examples shows how to use the RA API to
    # performan duration analysis, using both states and events.
    #
    # First we look at the state model of our session. We saw how to print it
    # out in example 00:
    ppheader("state models")
    pprint.pprint(session.describe('state_model'))

    # Let's say that we want to see for how long all the pilot(s) we use have
    # been active. Looking at the state model of the entity of type 'pilot' and
    # to the documentation of RADICAL-Pilot, we know that a pilot is active
    # between the state 'ACTIVE' and one of the three final states 'DONE',
    # 'CANCELED', 'FAILED' of all the entities of RP.
    ppheader("properties of the entities with etype 'Pipeline'")
    pipes = session.get(etype='Stage', uid='Stage.0000')
    pprint.pprint(pipes)