__copyright__ = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__ = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__license__ = "MIT"


# -----------------------------------------------------------------------------
# Possible states
#
INITIAL = 'DESCRIBED'
SCHEDULING = 'SCHEDULING'
SUSPENDED = 'SUSPENDED'
SCHEDULED = 'SCHEDULED'
SUBMITTING = 'SUBMITTING'
SUBMITTED = 'SUBMITTED'
COMPLETED = 'EXECUTED'
DEQUEUEING = 'DEQUEUEING'
DEQUEUED = 'DEQUEUED'
DONE = 'DONE'
FAILED = 'FAILED'
CANCELED = 'CANCELED'

# Final states
FINAL = [DONE, FAILED, CANCELED]

# AM: this should be
#   INITIAL = [DESCRIBED]
# so that tests for INITIAL and FINAL are symmetric.
#
# AM: DESCRIBED is NEW elsewhere in the RCT stack.
# AM: ENTK used FOOING / FOOED as states, RCT elsewhere uses FOO_PENDING, FOOING

# Assign numeric values to states
state_numbers = {

    INITIAL: 1,
    SCHEDULING: 2,
    SUSPENDED: 3,
    SCHEDULED: 4,
    SUBMITTING: 5,
    SUBMITTED: 6,
    COMPLETED: 7,
    DEQUEUEING: 8,
    DEQUEUED: 9,
    DONE: 10,
    FAILED: 10,
    CANCELED: 10
}


# States for Pipeline
_pipeline_state_values = {
    INITIAL: 1,
    SCHEDULING: 2,
    SUSPENDED: 3,
    DONE: 10,
    FAILED: 10,
    CANCELED: 10
}

_pipeline_state_inv = {}
for k, v in _pipeline_state_values.iteritems():
    if v in _pipeline_state_inv.keys():
        if not isinstance(_pipeline_state_inv[v], list):
            _pipeline_state_inv[v] = [_pipeline_state_inv[v]]
        _pipeline_state_inv[v].append(k)
    else:
        _pipeline_state_inv[v] = k

# States for Stage
_stage_state_values = {
    INITIAL: 1,
    SCHEDULING: 2,
    SCHEDULED: 4,
    DONE: 10,
    FAILED: 10,
    CANCELED: 10
}

_stage_state_inv = {}
for k, v in _stage_state_values.iteritems():
    if v in _stage_state_inv.keys():
        if not isinstance(_stage_state_inv[v], list):
            _stage_state_inv[v] = [_stage_state_inv[v]]
        _stage_state_inv[v].append(k)
    else:
        _stage_state_inv[v] = k

# States for Task
_task_state_values = {
    INITIAL: 1,
    SCHEDULING: 2,
    SCHEDULED: 4,
    SUBMITTING: 5,
    SUBMITTED: 6,
    COMPLETED: 7,
    DEQUEUEING: 8,
    DEQUEUED: 9,
    DONE: 10,
    FAILED: 10,
    CANCELED: 10
}

_task_state_inv = {}
for k, v in _task_state_values.iteritems():
    if v in _task_state_inv.keys():
        if not isinstance(_task_state_inv[v], list):
            _task_state_inv[v] = [_task_state_inv[v]]
        _task_state_inv[v].append(k)
    else:
        _task_state_inv[v] = k


