__copyright__   = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__      = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__     = "MIT"


# -----------------------------------------------------------------------------
# common states - Pipeline, Stage
UNSCHEDULED     = 'UNSCHEDULED'
SCHEDULING      = 'SCHEDULING'
SCHEDULED       = 'SCHEDULED'
EXECUTING       = 'EXECUTING'
DONE            = 'DONE'            # 'DONE', 'FAILED', 'CANCELED' correspond to various forms of 'EXECUTED'
FAILED          = 'FAILED'
CANCELED        = 'CANCELED'

# unique states - Tasks
QUEUEING        = 'QUEUEING'
QUEUED          = 'QUEUED'
DEQUEUEING      = 'DEQUEUEING'
DEQUEUED        = 'DEQUEUED'        # Dequeue thread will have this state
SYNCHRONIZING   = 'SYNCHRONIZING'
SYNCHRONIZED    = 'SYNCHRONIZED'    # Syncrhonizer thread will have this state

# shortcut
INITIAL  = [UNSCHEDULED]
FINAL = [DONE, FAILED, CANCELED]