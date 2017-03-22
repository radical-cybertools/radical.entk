__copyright__   = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__      = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__     = "MIT"


# -----------------------------------------------------------------------------
# common states
NEW      = 'UNSCHEDULED'
DONE     = 'DONE'
FAILED   = 'FAILED'
CANCELED = 'CANCELED'

# shortcut
INITIAL  = [NEW]
FINAL = [DONE, FAILED, CANCELED]


QUEUED = 'SCHEDULED'
EXECUTING = 'EXECUTING'
