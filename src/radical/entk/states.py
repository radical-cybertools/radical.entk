__copyright__ = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__ = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__license__ = "MIT"


# -----------------------------------------------------------------------------
# Possible states
INITIAL = 'DESCRIBED'
SCHEDULING = 'SCHEDULING'
SCHEDULED = 'SCHEDULED'
SUBMITTING = 'SUBMITTING'
SUBMITTED = 'SUBMITTED'
COMPLETED = 'EXECUTED'
DEQUEUEING = 'DEQUEUEING'
DEQUEUED = 'DEQUEUED'        
SYNCHRONIZING = 'SYNCHRONIZING'
SYNCHRONIZED = 'SYNCHRONIZED'
DONE = 'DONE'
FAILED = 'FAILED'
CANCELED = 'CANCELED'

# Final states
FINAL = [DONE, FAILED, CANCELED]

# Assign numeric values to states
state_numbers = {

    INITIAL: 1,
    SCHEDULING: 2,
    SCHEDULED: 3,
    SUBMITTING: 4,
    SUBMITTED: 5,
    COMPLETED: 6,
    DEQUEUEING: 7,
    DEQUEUED: 8,
    SYNCHRONIZING: 9,
    SYNCHRONIZED: 10,
    DONE: 11,
    FAILED: 11,
    CANCELED: 11
}


# States for Pipeline
_pipeline_state_values = {
    INITIAL: 1,
    SCHEDULING: 2,
    DONE: 11,
    FAILED: 11,
    CANCELED: 11
}

_pipeline_state_inv = {}
for k, v in _pipeline_state_values.iteritems():
    if v in _pipeline_state_inv.keys():
        if not isinstance(_pipeline_state_inv[v],list):
            _pipeline_state_inv[v] = [_pipeline_state_inv[v]]
        _pipeline_state_inv[v].append(k)
    else:
        _pipeline_state_inv[v] = k

# States for Stage
_stage_state_values = {
    INITIAL: 1,
    SCHEDULING: 2,
    SCHEDULED: 3,
    DONE:11,
    FAILED: 11,
    CANCELED: 11
}

_stage_state_inv = {}
for k, v in _stage_state_values.iteritems():
    if v in _stage_state_inv.keys():
        if not isinstance(_stage_state_inv[v],list):
            _stage_state_inv[v] = [_stage_state_inv[v]]
        _stage_state_inv[v].append(k)
    else:
        _stage_state_inv[v] = k

# States for Task
_task_state_values = {
    INITIAL: 1,
    SCHEDULING: 2,
    SCHEDULED: 3,
    SUBMITTING: 4,
    SUBMITTED: 5,
    COMPLETED: 6,
    DEQUEUEING: 7,
    DEQUEUED: 8,
    SYNCHRONIZING: 9,
    SYNCHRONIZED: 10,
    DONE: 11,
    FAILED: 11,
    CANCELED: 11
}

_task_state_inv = {}
for k, v in _task_state_values.iteritems():
    if v in _task_state_inv.keys():
        if not isinstance(_task_state_inv[v],list):
            _task_state_inv[v] = [_task_state_inv[v]]
        _task_state_inv[v].append(k)
    else:
        _task_state_inv[v] = k
