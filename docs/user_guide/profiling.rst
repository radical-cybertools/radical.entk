.. _uguide_profiling:

*********
Profiling
*********

EnTK can be configured to generate profiles by setting
``RADICAL_ENTK_PROFILE=True``. Profiles are generated per component and
sub-component. These profiles can be read and analyzed by using
`RADICAL Analytics (RA) <http://radicalanalytics.readthedocs.io>`_.

We describe profiling capabilities using RADICAL Analytics for EnTK via two
examples that extract durations and timestamps.

The scripts and the data can be found in your virtualenv under ``share/radical.entk/analytics/scripts``
or can be downloaded via the following links:

* Data: :download:`Link <../../examples/analytics/re.session.two.vivek.017759.0012.tar>`
* Durations: :download:`Link <../../examples/analytics/get_durations.py>`
* Timestamps: :download:`Link <../../examples/analytics/get_timestamps.py>`

Untar the data and run either of the scripts. We recommend following the inline
comments and output messages to get an understanding of RADICAL Analytics' usage
for EnTK.

More details on the capabilities of RADICAL Analytics can be found in its
`documentation <http://radicalanalytics.readthedocs.io>`_.

.. note:: The current examples of RADICAL Analytics are configured for RADICAL
    Pilot but can be changed to EnTK by
        * Setting `stype` to 'radical.entk' when creating the RADICAL Analytics session
        * Following the :ref:`state model <dev_docs_state_model>`, :ref:`event model <dev_docs_events>`, and :ref:`sequence diagram <dev_docs_seq_diag>` to determine the EnTK probes to use in profiling.

Extracting durations
====================

.. literalinclude:: ../../examples/analytics/get_durations.py
    :language: python
    :linenos:


Extracting timestamps
=====================

.. literalinclude:: ../../examples/analytics/get_timestamps.py
    :language: python
    :linenos: