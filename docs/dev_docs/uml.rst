.. _uml:

***
UML
***

This page presents the UML diagrams for the Ensemble Toolkit. Additional information to help understand 
the design of Ensemble Toolkit is also presented for interested developers.

Class Diagram
=============

The following document describes the classes in EnTK, their data members and functional members.

.. image:: ../figures/class_diagram.png
   :target: ../figures/class_diagram.png
   :width: 1200


Sequence Diagram
================

The interaction of these modules for one successful run of an application is described in the 
following figure:

.. image:: ../figures/sequence_diagram.png
   :target: ../figures/sequence_diagram.png


Communication Model
===================

The communication model of the various components in Ensemble Toolkit is presented below.

.. image:: ../figures/comm_model.jpg
   :target: ../figures/comm_model.jpg


Using RabbitMQ queues, the tmgr process, enqueue thread and dequeue thread communication the stateful objects
to the synchronizer thread in the master process. The synchronizer thread then sends acknowledgements (Ack) for each 
object received. The tmgr process, enqueue thread and dequeue thread are blocked till they receive an Ack and thus the 
the master process always remains in sync.

There also exist heartbeat messages between the heartbeat thread and the tmgr process in the TaskManager. This makes
sure our the tmgr is alive and the interaction with the runtime system is progressing.

.. image:: ../figures/heartbeat_model.jpg
   :target: ../figures/heartbeat_model.jpg