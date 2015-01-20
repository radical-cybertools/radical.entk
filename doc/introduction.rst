.. _introduction:

************
Introduction
************

What is EnsembleMD Toolkit?
===========================

The goal of Ensemble MD Toolkit is to provide an MD-specific alternative
to existing  High-Performance-Computing frameworks, libraries and workflow
systems. The majority of existing distributed computing solutions are generic
(by design) and provide a level of abstraction and interface that caters to
*any* concurrent, distributed  workflow. Consequently, MD workflows are often
modeled an executed via generic concurrent and distributed computing primitives.

Ensemble MD Toolkit takes a different approach. It provides a set of
explicit, predefined :ref:`patterns` that are commonly found in MD workflows.
Currently, these patterns are:

* (Bag-of-Tasks)
* Concurrent Pipeline
* Simulation-Analysis Loop
* Replica Exchange

Instead of defining tasks and their dependencies, users of Ensemble MD
Toolkit pick the pattern that represents their simulation's workflow and
populate it with :ref:`kernels`, an abstraction around MD tools, like
Amber, Gromacs, NAMD, etc.

The execution of the MD Kernels according to the pattern happens in the
background, transparently to the user. The mechanisms for resource allocations,
task submission and data transfer to one or more distributed execution hosts
are completely hidden from the users, so they can solely focus on optimizing
and improving the simulation workflow.


Concepts
========

Patterns
--------

Application Kernels
-------------------

Execution Environments
----------------------
