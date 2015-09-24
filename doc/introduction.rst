.. _introduction:

************
Introduction
************

What is Ensemble MD Toolkit?
============================

There are many science and engineering applications that require multiple
simulations and which can benefit from the ability to exploit task-level
parallelism. This includes the commonly known category of a "bag-of-tasks", where
by definition the simulations are not coupled. However, the degree-of-coupling
between the many simulations can differ, as measured by frequency of
interaction and volume of exchange between simulations.

The Ensemble MD Toolkit (EnsembleMD Toolkit) is a Python framework for
developing and executing applications which are comprised of many simulations, aka
ensembles. The Ensemble MD Toolkit has the following unique features: (i)
programming abstractions that enable the expression of ensembles as primary
entities, (ii) a well-defined API which provides support for the execution
patterns of ensembles, (iii) the ability to abstracts the execution details of these 
patterns from the expression of the patterns, and (iv) well-established runtime
capabilities to enable efficient and dynamic usage of resources ranging from
clouds, distributed clusters and supercomputers.

.. figure:: images/enmd_components.*
   :width: 360pt
   :align: center
   :alt: EnMD Components.

   `Figure 1: The Ensemble MD Toolkit`

EnsembleMD Toolkit provides a set of explicit, predefined patterns (see
:ref:`patterns`) that are commonly found in ensemble-based MD workflows.
Instead of defining tasks and their dependencies, EnsembleMD Toolkit
applications pick the pattern that represents the simulation workflow and
populate it with the "kernel abstraction" (see :ref:`api_kernels`) that captures
the MD engine of choice, like Amber, Gromacs, NAMD, etc. (see Figure 1). The
EnMD toolkit provides a useful balance between the free form scripting and the
some what restrictive environment of traditional workflows.

The patterns currently supported by EnsembleMD Toolkit are:

  * Pipeline
  * AllPairs
  * Simulation-Analysis Loop
  * Replica Exchange

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

  * :ref:`usecase_haussdorff`
  * :ref:`usecase_cdi_replica_exchange`
  * :ref:`usecase_extasy_coco_amber`
  * :ref:`usecase_extasy_gromacs_lsdmap`



Each of the pattern is uniquely targeted to a specific set of use-cases. While
the Pipeline and AllPairs patterns are fairly generic (they implement generic
algorithms), the ReplicaExchange and SimulationAnalysis patterns are much more
specific.  Some of the pattern behavior, specifically the degree of parallelism
can be controlled via parameters during object creation.

The execution of the MD Kernels according to the pattern happens in the
background, transparently to the user. The mechanisms for resource allocations,
task submission and data transfer to one or more distributed execution hosts
are completely hidden from the users, so they can solely focus on optimizing
and improving the simulation workflow.



"""To be moved to Getting Started Section

Concepts
========

Patterns
--------

A pattern is an object that represents a high-level application control flow. A
pattern can be seen as a parameterized template for an execution trajectory that
implements a specific algorithm. A pattern provides placeholder methods for the
individual steps and stages of an execution trajectory. These placeholders are
populated with Kernels that get executed when it’s the step’s / stages’ turn to
be executed.  The individual patterns provided in EnsembleMD can not be modified
by the user and they can not be nested. However, new patterns can be added to
EnsembleMD.

Application Kernels
-------------------

A kernel is an object that represents and abstracts a computational task in
EnsembleMD. A kernel can represent the invocation of a specific executable
(e.g., an executable from the Amber or NAMD suite of tools) or a more complex
invocation of compound tools.

Execution Environments
----------------------

An execution context is an object that represents a computing resource, i.e., in
most cases an HPC cluster. An execution context itself only represents the
concept (‘abstract base class’) and can have one or more concrete realizations.
Currently only one realization exists, the SingleClusterEnvironment,
representing a single resource. Typically, only one execution contexts object
exists throughout the lifetime of an EnsembleMD application. 
