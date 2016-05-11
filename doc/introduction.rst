.. _introduction:

************
Introduction
************

The Ensemble Toolkit is a Python framework for developing and executing applications 
comprised of many simulations, aka ensembles. The Ensemble Toolkit has the following unique 
features: (i) abstractions that enable the expression of ensembles as primary entities, (ii) an
API that provides support for ensembles-based execution pattern, (iii) the ability to abstract the execution details 
from the expression of the patterns, and (iv) well-established runtime capabilities to enable efficient 
and dynamic usage of resources ranging from clouds, distributed clusters and supercomputers.

We will now discuss the design and the components of Ensemble Toolkit  in order to understand how an application is created.

Design
==========

.. figure:: images/entk_arch.*
   :width: 360pt
   :align: center
   :alt: Ensemble Toolkit Architecture

   `Figure 1: The Ensemble Toolkit`

Ensemble Toolkit provides a set of explicit, predefined patterns (see :ref:`api_patterns`) that are commonly found in 
ensemble-based workflows. Instead of defining tasks and their dependencies, users can pick the pattern that 
represents the simulation workflow and populate it with the "kernels" (see :ref:`kern_api`) that captures 
the engine of choice, like Amber, Gromacs, NAMD, etc. (see Figure 1). The Ensemble toolkit provides a useful balance 
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

Kernels
--------------------------

A kernel is an object that abstracts a computational task in Ensemble Toolkit. It represents an instantiation of a specific 
science tool along with the required software environment. Kernel hide tool-specific peculiarities across 
different clusters as well as differences between the interfaces of the various MD tools to the extent possible. As part of 
Ensemble Toolkit a :ref:`kernels_list` is available. Users can also create custom kernels objects, discussed 
:ref:`here <writing_kernels>`

Resource Handle
----------------------------------

The resource handle is a representation of a computing infrastructure (CI), providing methods to:

* allocate resources
* run an execution pattern on these resources
* deallocate these resources

It is constructed with the information required to access the desired CI, i.e., its address, user credentials and core requirements, and 
URL to a database for book-keeping. Currently, only the SingleClusterEnvironment, which creates an resource handlet targetting one 
specific machine, is available. 

Execution Plugins (Internal Component)
------------------------------------------------------------

Ensemble Toolkit separates the expression of the application from the details of its execution. The user expresses the 
ensemble-based execution patterns, while the management of its execution is described by ”Execution Plugins”. The Execution plugin 
binds the Kernels and the Execution Patterns and translates them into executable units that are passed on to the underlying runtime 
system.

This type of decoupling of the execution from the expression of the pattern could potentially allow the execution plugins to 
analyze an application’s control- and datalow, combine the results with existing information about the execution resource and optimize 
along various parameters: total time to completion, amount of data transferred, throughput, etc.


Five steps to create an application
=======================

Each of the steps are labelled in Figure 1.

1. User picks an execution pattern that best represents their application and create an instance/object of the pattern class.
2. User selects Kernels for the various steps of the execution pattern: pre-defined or user-defined. These kernels also specify the data movement for that step.
3. User now creates a resource handle targetting a machine that would acquire a set of resources for a period of time.
4. Once the resource acquisition request is made, a) The pattern and the kernel are bound together in the execution plugins and translated into executable units b) Information from the resource is used to deploy these executable units on to the remote machine.
5. Once the application execution is completed, control goes back to the resource handle. The user can, now, run another pattern or deallocate the resources.
