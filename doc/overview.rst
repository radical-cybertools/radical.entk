.. _overview:

******************************
RADICAL EnsembleMD Overview
******************************

Who uses Ensemble MD Toolkit?
================================

While the concepts in EnsembleMD Toolkit are fairly generic to ensemble-based simulations, the framework 
currently targets  molecular dynamics simulations: current
patterns and kernel abstractions  directly address the
requirements for this community.

EnsembleMD Toolkit is being used in multiple molecular dynamic application
projects, like for example the `ExTASY Project <http://extasy-project.org/>`_.
We also maintain a set of use-cases that showcase different application
scenarios:

  * :ref:`usecase_haussdorff`
  * :ref:`usecase_cdi_replica_exchange`
  * :ref:`usecase_extasy_coco_amber`
  * :ref:`usecase_extasy_gromacs_lsdmap`

Each of the pattern is uniquely targeted to a specific set of use-cases. While
the Pipeline and AllPairs patterns are fairly generic (they implement generic
algorithms), the ReplicaExchange and SimulationAnalysis patterns are much more
specific.  Some of the pattern behavior, specifically the degree of parallelism
can be controlled via parameters during object creation.


Why do I need a MongoDB to run EnsembleMD ?
======================================================


How about data ?
===================

The Kernels have properties using which most data transfer methods can be performed. Data can be transferred from 
local to remote or data can be moved on the remote machine by linking or copying. Data can also be directly 
downloaded form an external location to the executing kernel. We will learn about these properties in the :ref:`user`.

