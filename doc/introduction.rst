.. _introduction:

************
Introduction
************

What is Ensemble MD Toolkit?
============================

There are many science and engineering applications that require multiple
simulations and which can benefit from the ability to exploit task-level
parallelism. This includes the commonly known category of a "bag-of-tasks"
where by definition the simulations are uncoupled. However, the degree-of-coupling
between the “many simulations” can differ, as measured by frequency of
interaction and volume of exchange between simulations.

The Ensemble MD Toolkit (EnMD Toolkit) is a Python framework for developing
and executing applications that are comprised of many simulations aka ensembles.
The EnMD Toolkit has the following unique features: (i) programming abstractions
that enable the expression of ensembles as primary entities, (ii) well defined
API which provides support for the execution patterns of ensembles, (ii)
abstracts the details of execution of these patterns from the expression of the
patterns, and (iv) -uses well-established runtime capabilities to enable
efficient and dynamic usage of resources ranging from clouds, distributed
clusters and supercomputers.

.. figure:: images/enmd_components.*
   :width: 360pt
   :align: center
   :alt: EnMD Components.

   `Figure 1: The EnsembleMD Toolkit`

EnMD Toolkit provides a set of explicit, predefined patterns (see
:ref:`the_patterns`) that are commonly found in ensemble-based MD workflows. Instead
of defining tasks and their dependencies, EnMD Toolkit applications pick the
pattern that represents their simulation’s workflow and populate it with the
"kernel abstraction" (see :ref:`the_kernels`) that captures the MD engine of choice,
like Amber, Gromacs, NAMD, etc. (see Figure 1). The EnMD toolkit provides a
useful balance between the free form scripting and the some what restrictive
environment of traditional workflows.


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
