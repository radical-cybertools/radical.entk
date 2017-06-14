__copyright__   = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__      = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__license__     = "MIT"


# -----------------------------------------------------------------------------
# common states - Pipeline, Stage
INITIAL         = 'DESCRIBED'
SCHEDULING      = 'SCHEDULING'
SCHEDULED       = 'SCHEDULED'
DONE            = 'DONE'            
FAILED          = 'FAILED'
CANCELED        = 'CANCELED'

# unique states - Tasks
SUBMITTING      = 'SUBMITTING'
SUBMITTED       = 'SUBMITTED'
COMPLETED       = 'COMPLETED'
DEQUEUEING      = 'DEQUEUEING'
DEQUEUED        = 'DEQUEUED'        # Dequeue thread will have this state
SYNCHRONIZING   = 'SYNCHRONIZING'
SYNCHRONIZED    = 'SYNCHRONIZED'    # Syncrhonizer thread will have this state

# shortcut
FINAL = [DONE, FAILED, CANCELED]


## Assign numeric values to states
state_numbers = {
    
    INITIAL         : 1,
    SCHEDULING      : 2,
    SCHEDULED       : 3,
    SUBMITTING      : 4,
    SUBMITTED       : 5,
    COMPLETED       : 6,
    DEQUEUEING      : 7,
    DEQUEUED        : 8,
    SYNCHRONIZING   : 9,
    SYNCHRONIZED    : 10,
    DONE            : 11,
    FAILED          : 11,
    CANCELED        : 11
}

## Get back string values from numeric values for states
state_strings = {}
for k,v in state_numbers.iteritems():
    state_strings[v] = k