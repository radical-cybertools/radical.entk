.. _library:

*************
API Reference
*************


Pipleline Pattern API
=====================

.. autoclass:: radical.ensemblemd.Pipeline
    :members:
    :inherited-members:

SimulationAnalysisLoop Pattern API
==================================

.. autoclass:: radical.ensemblemd.SimulationAnalysisLoop
    :members:
    :inherited-members:

ReplicaExchange Pattern API
===========================

.. autoclass:: radical.ensemblemd.ReplicaExchange
    :members:
    :inherited-members:

Execution Context API
=====================

.. autoclass:: radical.ensemblemd.SingleClusterEnvironment
    :members:
    :inherited-members:

.. .. autoclass:: radical.ensemblemd.MultiClusterEnvironment
..     :members:
..     :inherited-members:

Application Kernel API
======================

.. autoclass:: radical.ensemblemd.Kernel
    :members:
    :inherited-members:

Exceptions & Errors
===================

.. automodule:: radical.ensemblemd.exceptions
   :show-inheritance:
   :members: EnsemblemdError, NotImplementedError, TypeError, ArgumentError, FileError, NoKernelPluginError, NoKernelConfigurationError, NoExecutionPluginError
