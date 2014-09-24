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
    a particular kernel and scheme
    """
    def __init__(self, my_id):
        """Constructor.

        Arguments:
        my_id - integer representing replica's id
        """
        self.id = int(my_id)


# ------------------------------------------------------------------------------
#
class ReplicaExchange(ExecutionPattern):
    
    #---------------------------------------------------------------------------
    #
    def __init__(self):
        """
        """
        super(ReplicaExchange, self).__init__()
        self.replica_objects = None

    #-------------------------------------------------------------------------------
    #
    def get_name(self):
        """Implements base class ExecutionPattern.get_name().
        """
        return PATTERN_NAME

    #-------------------------------------------------------------------------------
    #
    def initialize_replicas(self):
        """Initializes replicas and their attributes to default values.
        Currently incomplete! Can be complete if we assume only temperature
        exchange RE, otherwise RE type must be passed as parameter.
        """
        raise NotImplementedError(method_name="initialize_replicas", class_name=type(self))

    #-------------------------------------------------------------------------------
    #
    def add_replicas(self, replicas):
        """
        """
        self.replica_objects = replicas

    #-------------------------------------------------------------------------------
    #
    def get_replicas(self):
        """
        """
        return self.replica_objects

    #-------------------------------------------------------------------------------
    #
    def compose_swap_matrix(self, replicas):
        """Creates a swap matrix from matrix_column_x.dat files. 
        matrix_column_x.dat - is populated on targer resource and then transferred back. This
        file is created for each replica and has data for one column of swap matrix. In addition 
        to that, this file holds path to pilot compute unit of the previous run, where reside 
        output files for a given replica. There are slight variations based on scheme type, but 
        this function is kernel independent. 

        Arguments:
        replicas - list of Replica objects

        Returns:
        swap_matrix - 2D list of lists of dimension-less energies, where each column is a replica 
        and each row is a state
        """

        raise NotImplementedError(method_name="compose_swap_matrix", class_name=type(self))

    #-------------------------------------------------------------------------------
    #
    def build_input_file(self, replica):
        """Generates input file for individual replica, based on template input file

        Arguments:
        replica - Replica object
        """

        raise NotImplementedError(method_name="build_input_file", class_name=type(self))

    #-------------------------------------------------------------------------------
    #
    def prepare_replica_for_md(self, replica):
        """Creates a list of radical.ensemblemd.Kernel objects for MD simulation step. Here are
        specified input/output files to be transferred to/from target resource, paths to
        files on resource an MD kernel has to read data from and other details associated
        with execution of MD step

        Arguments:
        replica - Replica object

        Returns:
        compute_replica - radical.ensemblemd.Kernel object
        """

        raise NotImplementedError(method_name="prepare_replica_for_md", class_name=type(self))

    # ------------------------------------------------------------------------------
    #
    def prepare_replica_for_exchange(self, replica):
        """Creates a list of radical.ensemblemd.Kernel objects for exchange step to be
        performed on a target resource. 

        Arguments:
        replica - Replica objects

        Returns:
        exchange_replica - radical.ensemblemd.Kernel object
        """

        raise NotImplementedError(method_name="prepare_replica_for_exchange", class_name=type(self))
    
    #--------------------------------------------------------------------------------
    #
    def exchange(self, r_i, replicas, swap_matrix):
        """Produces a replica "j" to exchange with the given replica "i"
        based off independence sampling of the discrete distribution

        Arguments:
        r_i - given replica for which is found partner replica
        replicas - list of Replica objects
        swap_matrix - matrix of dimension-less energies, where each column is a replica 
        and each row is a state

        Returns:
        r_j - replica to exchnage parameters with
        """
        raise NotImplementedError(method_name="exchange", class_name=type(self))
    
        
