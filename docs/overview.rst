.. _overview:

*************
The PST Model
*************

In this page, we provide details on how the Pipeline, Stage and Task (PST) components can be used to create 
applications with various task workflows. 

.. figure:: figures/pst-model.jpg
   :width: 400pt
   :align: center
   :alt: Ensemble Toolkit Design - high level

   `Figure 1: PST Model`


We consider two pythonic collections of objects, **Sets** and **Lists**, to describe the task workflow. A 
**set**-based object represents entities that have **no relative order** and hence spacially independent. 
A **list**-based object represents entities that have a linear **temporal** order, i.e. entity 'i' can only be
operated after entity 'i-1'. 

In our PST model, we have the following objects:

* **Task**: description of an executing kernel 
* **Stage**: our set-based object where the entities are Tasks, i.e. a Stage is a set of Tasks
* **Pipeline**: our list-based object where the entities are Stages, i.e. a Pipeline is a list of Stages

The entire application when assigned to the Application Manager can be expressed as a set of Pipelines. A graphical
representation of an application is provided in Figure 2. Note how different Pipelines can have different number of 
Stages and different Stages can have different number of Tasks.

By expressing your application as a set or *list* of such Pipelines, one can create any task workflow that can be 
expressed as a DAG.
