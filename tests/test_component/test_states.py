from radical.entk import states


def test_states_list():

    assert states.INITIAL == 'DESCRIBED'
    assert states.SCHEDULING == 'SCHEDULING'
    assert states.SUSPENDED == 'SUSPENDED'
    assert states.SCHEDULED == 'SCHEDULED'
    assert states.SUBMITTING == 'SUBMITTING'
    assert states.SUBMITTED == 'SUBMITTED'
    assert states.COMPLETED == 'EXECUTED'
    assert states.DEQUEUEING == 'DEQUEUEING'
    assert states.DEQUEUED == 'DEQUEUED'
    assert states.DONE == 'DONE'
    assert states.FAILED == 'FAILED'
    assert states.CANCELED == 'CANCELED'
    assert states.FINAL == [states.DONE, states.FAILED, states.CANCELED]


def test_state_numeric_vals():

    assert states.state_numbers == {states.INITIAL: 1,
                                    states.SCHEDULING: 2,
                                    states.SUSPENDED: 3,
                                    states.SCHEDULED: 4,
                                    states.SUBMITTING: 5,
                                    states.SUBMITTED: 6,
                                    states.COMPLETED: 7,
                                    states.DEQUEUEING: 8,
                                    states.DEQUEUED: 9,
                                    states.DONE: 10,
                                    states.FAILED: 10,
                                    states.CANCELED: 10
                                    }


def test_pipeline_states():

    assert states._pipeline_state_values == {states.INITIAL: 1,
                                             states.SCHEDULING: 2,
                                             states.SUSPENDED: 3,
                                             states.DONE: 10,
                                             states.FAILED: 10,
                                             states.CANCELED: 10
                                             }

    assert states._pipeline_state_inv == {1: states.INITIAL,
                                          2: states.SCHEDULING,
                                          3: states.SUSPENDED,
                                          10: [states.FAILED, states.CANCELED, states.DONE]
                                          }


def test_stage_states():

    assert states._stage_state_values == {states.INITIAL: 1,
                                          states.SCHEDULING: 2,
                                          states.SCHEDULED: 4,
                                          states.DONE: 10,
                                          states.FAILED: 10,
                                          states.CANCELED: 10
                                          }
    assert states._stage_state_inv == {1: states.INITIAL,
                                       2: states.SCHEDULING,
                                       4: states.SCHEDULED,
                                       10: [states.FAILED, states.CANCELED, states.DONE]
                                       }


def test_task_states():

    assert states._task_state_values == {states.INITIAL: 1,
                                         states.SCHEDULING: 2,
                                         states.SCHEDULED: 4,
                                         states.SUBMITTING: 5,
                                         states.SUBMITTED: 6,
                                         states.COMPLETED: 7,
                                         states.DEQUEUEING: 8,
                                         states.DEQUEUED: 9,
                                         states.DONE: 10,
                                         states.FAILED: 10,
                                         states.CANCELED: 10
                                         }

    assert states._task_state_inv == {
        1: states.INITIAL,
        2: states.SCHEDULING,
        4: states.SCHEDULED,
        5: states.SUBMITTING,
        6: states.SUBMITTED,
        7: states.COMPLETED,
        8: states.DEQUEUEING,
        9: states.DEQUEUED,
        10: [states.DONE, states.CANCELED, states.FAILED]
    }
