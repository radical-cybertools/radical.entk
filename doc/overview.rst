.. _overview:

*************************************
RADICAL Ensemble Toolkit Overview
*************************************

Who uses Ensemble Toolkit?
================================


Ensemble Toolkit is being used in multiple molecular dynamic application
projects, like for example the `ExTASY Project <http://extasy-project.org/>`_.
We also maintain a set of use-cases that showcase different application
scenarios:

  * :ref:`usecase_haussdorff`
  * :ref:`usecase_cdi_replica_exchange`
  * :ref:`usecase_extasy_coco_amber`
  * :ref:`usecase_extasy_gromacs_lsdmap`

While the concepts in Ensemble Toolkit are fairly generic to ensemble-based simulations, the framework 
currently targets  molecular dynamics simulations: current patterns and kernel abstractions directly address the requirements for this community.


How about data ?
===================

The Kernels have properties using which most data transfer methods can be performed. Data can be transferred from 
local to remote or data can be moved on the remote machine by linking or copying. Data can also be directly 
downloaded form an external location to the executing kernel. We will learn about these properties in the :ref:`user`.

