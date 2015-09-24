#!/usr/bin/env python

"""This module defines a Replica Exchange pattern.
"""

__author__    = "Antons Treikalis <antons.treikalis@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.ensemblemd.exceptions import NotImplementedError
from radical.ensemblemd.execution_pattern import ExecutionPattern

PATTERN_NAME = "ReplicaExchange"

class Replica(object):
    """Class representing replica and it's associated data.

    This will have to be extended by users implementing RE pattern for
    a particular kernel and execution pattern
    """
    def __init__(self, my_id):
        """Constructor.

        Arguments:
        my_id - integer representing id of this replica
        """
        self.id = int(my_id)


# ------------------------------------------------------------------------------
#
class ReplicaExchange(ExecutionPattern):
    """ Replica Exchange pattern.

            .. image:: ../../images/replica_exchange_pattern.*
               :width: 300pt
    """
    def __init__(self):
        """Constructor.
        """
        super(ReplicaExchange, self).__init__()
        self._replica_objects = None

    #---------------------------------------------------------------------------
    #
    @property
    def name(self):
        """Returns the name of the execution pattern.
        """
        return PATTERN_NAME

    #---------------------------------------------------------------------------
    #
    def initialize_replicas(self):
        """Initializes replicas and their attributes to default values.
        """
        raise NotImplementedError(method_name="initialize_replicas", \
                                  class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def add_replicas(self, replicas):
        """Adds initialised replicas to this pattern.

        Arguments:
        replicas - list of replica objects
        """
        self.replica_objects = replicas

    #---------------------------------------------------------------------------
    #
    def get_replicas(self):
        """Returns a list of replica objects associated with this pattern.
        """
        return self.replica_objects

    #---------------------------------------------------------------------------
    #
    def get_swap_matrix(self, replicas, matrix_columns):
        """Creates and populates swap matrix, which is used to determine 
        exchange probabilities.

        Arguments:
        replicas - list of Replica objects
        matrix_columns - matrix of energy parameters obtained during the 
        exchange step

        Returns:
        swap_matrix - 2D list of lists of dimension-less energies, where each 
        column is a replica and each row is a state
        """

        raise NotImplementedError(method_name="compose_swap_matrix", \
                                  class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def perform_swap(self, replica_i, replica_j):
        """Performs an exchange of replica parameters

        Arguments:
        replica_i - a replica object
        replica_j - a replica object
        """

        raise NotImplementedError(method_name="compose_swap_matrix", \
                                  class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def build_input_file(self, replica):
        """Generates input file for individual replica, based on template input 
        file

        Arguments:
        replica - Replica object
        """

        raise NotImplementedError(method_name="build_input_file", \
                                  class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def prepare_replica_for_md(self, replica):
        """Creates a list of radical.ensemblemd.Kernel objects for MD simulation 
        step. Here are specified input/output files to be transferred to/from 
        target resource and other details associated with execution of MD step.

        Arguments:
        replica - Replica object

        Returns:
        compute_replica - radical.ensemblemd.Kernel object
        """

        raise NotImplementedError(method_name="prepare_replica_for_md", \
                                  class_name=type(self))

    # --------------------------------------------------------------------------
    #
    def prepare_replica_for_exchange(self, replica):
        """Creates a list of radical.ensemblemd.Kernel objects for exchange step 
        to be performed on a target resource.

        Arguments:
        replica - Replica object

        Returns:
        exchange_replica - radical.ensemblemd.Kernel object
        """

        raise NotImplementedError(method_name="prepare_replica_for_exchange", \
                                  class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def exchange(self, r_i, replicas, swap_matrix):
        """Produces a replica "j" for exchange with given replica "i"

        Arguments:
        r_i - given replica for which is found partner replica
        replicas - list of Replica objects
        swap_matrix - matrix of dimension-less energies, where each column is a 
        replica and each row is a state

        Returns:
        r_j - replica to exchnage parameters with
        """
        raise NotImplementedError(method_name="exchange", class_name=type(self))

