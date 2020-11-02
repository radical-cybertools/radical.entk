# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

from unittest     import TestCase
from radical.entk import states


# ------------------------------------------------------------------------------
#
class TestBase(TestCase):

    # --------------------------------------------------------------------------
    #
    def test_states_list(self):

        self.assertEqual(states.INITIAL,    'DESCRIBED')
        self.assertEqual(states.SCHEDULING, 'SCHEDULING')
        self.assertEqual(states.SUSPENDED,  'SUSPENDED')
        self.assertEqual(states.SCHEDULED,  'SCHEDULED')
        self.assertEqual(states.SUBMITTING, 'SUBMITTING')
        self.assertEqual(states.COMPLETED,  'EXECUTED')
        self.assertEqual(states.DONE,       'DONE')
        self.assertEqual(states.FAILED,     'FAILED')
        self.assertEqual(states.CANCELED,   'CANCELED')
        self.assertEqual(states.FINAL,
                         [states.DONE, states.FAILED, states.CANCELED])


    # --------------------------------------------------------------------------
    #
    def test_state_numeric_vals(self):

        self.assertEqual(states.state_numbers,
                         {
                             states.INITIAL   :  1,
                             states.SCHEDULING:  2,
                             states.SUSPENDED :  3,
                             states.SCHEDULED :  4,
                             states.SUBMITTING:  5,
                             states.COMPLETED :  7,
                             states.DONE      : 10,
                             states.FAILED    : 10,
                             states.CANCELED  : 10
                         })


    # --------------------------------------------------------------------------
    #
    def test_pipeline_states(self):

        self.assertEqual(states._pipeline_state_values,
                         {
                             states.INITIAL    :  1,
                             states.SCHEDULING :  2,
                             states.SUSPENDED  :  3,
                             states.DONE       : 10,
                             states.FAILED     : 10,
                             states.CANCELED   : 10
                         })

        self.assertEqual(states._pipeline_state_inv,
                         {
                             1: states.INITIAL,
                             2: states.SCHEDULING,
                             3: states.SUSPENDED,
                             10: [states.DONE, states.FAILED, states.CANCELED]
                         })


    # --------------------------------------------------------------------------
    #
    def test_stage_states(self):

        self.assertEqual(states._stage_state_values,
                         {
                             states.INITIAL    :  1,
                             states.SCHEDULING :  2,
                             states.SCHEDULED  :  4,
                             states.DONE       : 10,
                             states.FAILED     : 10,
                             states.CANCELED   : 10
                         })
        self.assertEqual(states._stage_state_inv,
                         {
                             1: states.INITIAL,
                             2: states.SCHEDULING,
                             4: states.SCHEDULED,
                             10: [states.DONE, states.FAILED, states.CANCELED]
                         })


    # --------------------------------------------------------------------------
    #
    def test_task_states(self):

        self.assertEqual(states._task_state_values,
                         {
                             states.INITIAL    :  1,
                             states.SCHEDULING :  2,
                             states.SCHEDULED  :  4,
                             states.SUBMITTING :  5,
                             states.COMPLETED  :  7,
                             states.DONE       : 10,
                             states.FAILED     : 10,
                             states.CANCELED   : 10
                         })

        self.assertEqual(states._task_state_inv,
                         {
                              1: states.INITIAL,
                              2: states.SCHEDULING,
                              4: states.SCHEDULED,
                              5: states.SUBMITTING,
                              7: states.COMPLETED,
                             10: [states.DONE, states.FAILED, states.CANCELED]
                         })
