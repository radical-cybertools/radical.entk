.. _uml:

********************************
UML diagrams and other resources
********************************

This page presents the UML diagrams for the Ensemble Toolkit. Additional information to help understand 
the design of Ensemble Toolkit is also presented for interested developers.


.. _dev_docs_cls_diag:

Class Diagram
=============

The following document describes the classes in EnTK, their data members and functional members.

.. image:: ../figures/entk_class_diagram.jpg
   :target: ../figures/entk_class_diagram.jpg
   :width: 1200


.. _dev_docs_seq_diag:

Sequence Diagram
================

The interaction of these modules for one successful run of an application is described in the 
following figure:

.. image:: ../figures/entk_sequence_diagram.jpg
   :target: ../figures/entk_sequence_diagram.jpg


.. _dev_docs_state_model:

State Diagram
=============

The stateful objects Pipeline, Stage and Task undergo several state transition during the execution of a workflow. We 
document them in the following figure:

.. image:: ../figures/entk_state_diagram.jpg
   :target: ../figures/entk_state_diagram.jpg


.. _dev_docs_comm_model:

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


.. _dev_docs_events:

Events Recorded
===============

Following is a list of events recorded in each module (in temporal order). These events can be used to profile durations between two events
or a state and an event.

**Resource Manager**

.. code-block:: bash

    create rmgr
    validating rdesc
    rdesc validated
    populating rmgr
    rmgr populated
    rmgr created
    creating rreq
    rreq created
    rreq submitted
    resource active
    canceling resource allocation
    resource allocation canceled

**App Manager**

.. code-block:: bash

    create amgr
    amgr created
    assigning workflow
    validating workflow
    workflow validated
    amgr run started
    init mqs setup
    mqs setup done
    init rreq submission
    starting synchronizer thread
    creating wfp obj
    creating tmgr obj
    synchronizer thread started
    start termination
    terminating synchronizer
    termination done

**WFprocessor**

.. code-block:: bash

    create wfp obj
    wfp obj created
    creating wfp process
    starting wfp process
    wfp process started
    creating dequeue-thread
    starting dequeue-thread
    creating enqueue-thread
    starting enqueue-thread
    dequeue-thread started
    enqueue-thread started
    terminating dequeue-thread
    terminating enqueue-thread
    termination done
    terminating wfp process
    wfp process terminated

**TaskManager**

.. code-block:: bash

    create tmgr obj
    tmgr obj created
    creating heartbeat thread
    starting heartbeat thread
    heartbeat thread started
    creating tmgr process
    starting tmgr process
    tmgr process started
    tmgr infrastructure setup done
    cud from task - create
    cud from task - done
    task from cu - create
    task from cu - done
    terminating tmgr process
    tmgr process terminated
