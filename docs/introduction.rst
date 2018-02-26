.. _introduction:

************
Introduction
************

Overview
========


The Ensemble Toolkit is a Python framework for developing and executing 
applications comprised of multiple sets of tasks, aka ensembles. Ensemble 
Toolkit was originally developed with ensemble-based applications in mind. As 
our understanding of the variety of workflows in scientific application 
improved, we realized our approach needs to be more generic. Although our 
motivation remains that of Ensemble-based applications, from EnTK 0.6 onwards, 
any application where the task workflow can be expressed as a Directed Acyclic 
Graph, can be supported.

The Ensemble Toolkit has the following unique features: (i) abstractions that 
enable the expression of various task graphs, (ii) abstraction of resource 
management and task execution, (iii) Fault tolerance as a first order concern
and (iv) well-established runtime capabilities to enable efficient and dynamic 
usage of grid resources and supercomputers.

We will now discuss the high level design of Ensemble Toolkit in order to 
understand how an application is created and executed.

Design
------

.. figure:: figures/design-high-level.jpg
   :width: 500pt
   :align: center
   :alt: Ensemble Toolkit Design - high level

   `Figure 1: High level design of Ensemble Toolkit`


Ensemble toolkit consists of several components that serve different purposes. 
There are four user level components, namely, Resource Manager, Pipeline, Stage
and Task, that are used directly by the user. The **Resource Manager** is used
to describe the resource to be used for execution of the application. The 
**Pipeline**, **Stage** and **Task** are components used to create the 
application by describing its task graph. We will soon take a look into how 
these can be used to create an application.

The **Application Manager** is an internal components, that takes the workflow 
described by the user and converts them into a set of **workloads**, i.e. tasks
with no dependencies by parsing through the workflow and identifying, during 
runtime, tasks with equivalent or no dependencies. 

The **Task Execution Manager** is the last component in Ensemble Toolkit. It 
accepts the workload prepared by the Application Manager and executes them on 
the specified resource using a Runtime system.

Ensemble Toolkit uses a runtime system as a framework to simply execute tasks 
on computing infrastructures (CIs). The runtime system is expected to manage 
the direct interactions with the various software and hardware layers of the 
CIs, including the heterogeneity amongst various CIs.

More details about how EnTK is designed and implemented can be found 
:ref:`here <entk>`.

.. _dependency:

Dependencies
------------

Ensemble Toolkit uses `RADICAL Pilot (RP) <http://radicalpilot.readthedocs.org>`_ 
as the runtime system. RP is targeted currently only for a set of high 
performance computing (HPC) systems 
(`see here <http://radicalpilot.readthedocs.io/en/latest/resources.html#chapter-resources>`_). 
RP can be extended to support more HPC systems by contacting the developers of 
RP/EnTK or by the user themselves by following 
`this page <http://radicalpilot.readthedocs.io/en/latest/machconf.html#writing-a-custom-resource-configuration-file>`_.

Ensemble Toolkit also relies on RabbitMQ to support messaging capabilities 
between its various components. Read more about it `here <http://www.rabbitmq.com/>`_.

EnTK also has profiling capabilities and uses `Pandas <https://pandas.pydata.org/>`_ 
dataframes to store the data. Users can use these dataframes to wrangle the data
or directly plot the required fields.

Dependencies such as RP and Pandas are automatically installed when installing 
EnTK. RabbitMQ, on the other hand, needs to be installed manually by the user.
Instructions for all are provided in :ref:`here <installation>`.

Five steps to create an application
-----------------------------------

1. Use the Pipeline, Stage and Task components to create the workflow
2. Describe the CI to be used via the Resource Manager
3. Create an Application Manager object and assign the Resource Manager and the workflow to it.
4. Run the Application Manager
5. Sit back and relax!

Jump ahead to take a look at the step by step instructions for an example
script :ref:`here <uguide_get_started>`.

Intended users
==============

Ensemble Toolkit is completely python based and requires familiarity with the 
python language. 

Our primary focus is to support domain scientists and enable them to execute 
their applications at scale on various of CI. But this does not mean this 
framework cannot be used by users with simpler requirements. Even if no HPC is 
to be used, consider using EnTK for its automation and fault-tolerance 
capabilities (even on your personal PC)!

Some of our current users are mentioned below.

+------------------------+------------+
| User Groups            |   Domain   |
+========================+============+
| University of Colorado,|  Molecular |
| Denver                 |  Dynamics  |
+------------------------+------------+
| Penn State University  | Climate    |
|                        | Science    |
+------------------------+------------+
| Princeton University   | Seismology |
+------------------------+------------+
| University College of  | Molecular  |
| London                 | Dynamics,  |
|                        | Medicine   |
+------------------------+------------+
| Rice University        | Molecular  |
|                        | Dynamics   |
+------------------------+------------+
