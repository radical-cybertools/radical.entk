.. _introduction:

************
Introduction
************

The Ensemble MD Toolkit (EnsembleMD Toolkit) is a Python framework for developing and executing applications 
comprised of many simulations, aka ensembles. The Ensemble MD Toolkit has the following unique 
features: (i) abstractions that enable the expression of ensembles as primary entities, (ii) an
API that provides support for ensembles-based execution pattern, (iii) the ability to abstract the execution details 
from the expression of the patterns, and (iv) well-established runtime capabilities to enable efficient 
and dynamic usage of resources ranging from clouds, distributed clusters and supercomputers.

We will now discuss the design and the components of Ensemble MD in order to understand how an application is created.

Design
==========

.. figure:: images/enmdtk_arch.*
   :width: 360pt
   :align: center
   :alt: EnMD Architecture

   `Figure 1: The Ensemble MD Toolkit`

EnsembleMD Toolkit provides a set of explicit, predefined patterns (see :ref:`api_patterns`) that are commonly found in 
ensemble-based MD workflows. Instead of defining tasks and their dependencies, users can pick the pattern that 
represents the simulation workflow and populate it with the "kernels" (see :ref:`kern_api`) that captures 
the MD engine of choice, like Amber, Gromacs, NAMD, etc. (see Figure 1). The EnMD toolkit provides a useful balance 
between the free form scripting and the some what restrictive environment of traditional workflows.


The execution of the kernels according to the pattern happens in the background, transparently to the user. The 
mechanisms for resource allocations, task submission and data transfer to one or more distributed execution hosts
are completely hidden from the users, so they can solely focus on optimizing and improving the simulation workflow.


Components
===============

Execution Patterns
--------------------------------

An execution pattern is a high-level object that describes "what to do", i.e. represents the application control flow. An execution pattern 
represents the pattern in terms of multiple "steps" and provides placeholder methods for each of the individual steps.These placeholders 
are populated with Kernels that get executed when it’s the step’s turn to be executed. 

Kernel Plugins
--------------------------

A kernel is an object that abstracts a computational task in EnsembleMD. It represents an instantiation of a specific 
science tool along with its resource specific environment. Kernel plugins hide tool-specific peculiarities across 
different clusters as well as differences between the interfaces of the various MD tools to the extent possible. As part of 
EnsembleMD a :ref:`kernels_list` is available. Users can also create custom kernels by creating kernel plugins, discussed 
:ref:`here <writing_kernels>`

Execution Context
----------------------------------

The execution context is a representation of a computing infrastructure (CI), providing methods to:

* allocate resources
* run an execution pattern on these resources
* deallocate these resources

It is constructed with the information required to access the desired CI, i.e., its address, user credentials and core requirements, and 
URL to a database for book-keeping. Currently, only the SingleClusterEnvironment, which creates an execution context targetting one 
specific machine, is available. 

Execution Plugins (Internal Component)
------------------------------------------------------------

Ensemble MD Toolkit separates the expression of the application from the details of its execution. The user expresses the 
ensemble-based execution patterns, while the management of its execution is described by ”Execution Plugins”. The Execution plugins 
consist of task level representation of the Kernel of each step that are scheduled, for execution, as per the selected Execution Pattern.

This type of decoupling of the execution from the expression of the pattern could potentially allow the execution plugins to 
analyze an application’s control- and datalow, combine the results with existing information about the execution resource and optimize 
along various parameters: total time to completion, amount of data transferred, throughput, etc.


Five steps to create an application
=======================

Each of the steps are labelled in Figure 1.

1. User picks an execution pattern that best represents their application and create an instance/object of the pattern class.
2. The various steps of the execution pattern can now be filled with Kernels: pre-defined or user-defined. These kernels also specify the data movement for that step.
3. Users now create an execution context targetting a machine that would acquire a set of resources for a period of time.
4. Once the resource acquisition request is made, a) the pattern class instance/object is "run" via the execution context on the remote machine. b) This converts the execution pattern into execution plugins which consist of RADICAL Pilot constructs.
5. Once the application execution is completed, control goes back to the execution context. The user can, now, run another pattern or deallocate the resources.
