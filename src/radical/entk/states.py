__copyright__   = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__      = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__     = "MIT"


import os

# -----------------------------------------------------------------------------
# common states
NEW      = 'NEW'
DONE     = 'DONE'
FAILED   = 'FAILED'
CANCELED = 'CANCELED'

# shortcut
INITIAL  = [NEW]
FINAL = [DONE, FAILED, CANCELED]


QUEUED = 'QUEUED_LOCALLY'