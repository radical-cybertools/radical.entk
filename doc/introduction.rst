.. _introduction:

************
Introduction
************

What is Ensemble MD Toolkit?
============================

There are many science and engineering applications that require multiple
simulations and which can benefit from the ability to exploit task-level
parallelism. This includes the commonly known category of a "bag-of-tasks" where
by definition the simulations are uncoupled. However, the degree-of-coupling
between the “many simulations” can differ, as measured by frequency of
interaction and volume of exchange between simulations.

The Ensemble MD Toolkit (Ensemble MD Toolkit) is a Python framework for
developing and executing applications that are comprised of many simulations aka
ensembles. The Ensemble MD Toolkit has the following unique features: (i)
programming abstractions that enable the expression of ensembles as primary
entities, (ii) well defined API which provides support for the execution
patterns of ensembles, (ii) abstracts the details of execution of these patterns
from the expression of the patterns, and (iv) -uses well-established runtime
capabilities to enable efficient and dynamic usage of resources ranging from
clouds, distributed clusters and supercomputers.

.. figure:: images/enmd_components.*
   :width: 360pt
   :align: center
   :alt: EnMD Components.

   `Figure 1: The Ensemble MD Toolkit`

Ensemble MD Toolkit provides a set of explicit, predefined patterns (see
:ref:`the_patterns`) that are commonly found in ensemble-based MD workflows.
Instead of defining tasks and their dependencies, Ensemble MD Toolkit
applications pick the pattern that represents their simulation’s workflow and
populate it with the "kernel abstraction" (see :ref:`the_kernels`) that captures
the MD engine of choice, like Amber, Gromacs, NAMD, etc. (see Figure 1). The
EnMD toolkit provides a useful balance between the free form scripting and the
some what restrictive environment of traditional workflows.

Who uses Ensemble MD Toolkit?
=============================

While the concepts in EnsembleMD Toolkit are fairly generic, the framework is
currently targeted towards the molecular dynamics community: the currently
implemented patterns and kernel abstractions are directly addressing the
requirements for this community.

EnsembleMD Toolkit is being used in multiple molecular dynamic application
projects, like for example the `ExTASY Project <http://extasy-project.org/>`_.
We also maintain a set of use-cases that showcase different application
scenarios:

  * :ref:`usecase_cdi_replica_exchange`
  * :ref:`usecase_extasy_coco_amber`
  * :ref:`usecase_extasy_gromacs_lsdmap`
  * :ref:`usecase_haussdorff`

.. Ensemble MD Toolkit takes a different approach. It provides a set of
.. explicit, predefined :ref:`patterns` that are commonly found in MD workflows.
.. Currently, these patterns are:
..
..   * (Bag-of-Tasks)
..   * Pipeline
..   * Simulation-Analysis Loop
..   * Replica Exchange
..
.. Instead of defining tasks and their dependencies, users of Ensemble MD
.. Toolkit pick the pattern that represents their simulation's workflow and
.. populate it with :ref:`kernels`, an abstraction around MD tools, like
.. Amber, Gromacs, NAMD, etc.
..
.. The execution of the MD Kernels according to the pattern happens in the
.. background, transparently to the user. The mechanisms for resource allocations,
.. task submission and data transfer to one or more distributed execution hosts
.. are completely hidden from the users, so they can solely focus on optimizing
.. and improving the simulation workflow.


.. Concepts
.. ========
..
.. Patterns
.. --------
..
.. Application Kernels
.. -------------------
..
.. Execution Environments
.. ----------------------
