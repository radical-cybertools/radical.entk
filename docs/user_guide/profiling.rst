.. _uguide_profiling:

*********
Profiling
*********

EnTK can be configured to generate profiles by setting 
``RADICAL_ENTK_PROFILE=True``. Profiles are generated per component and
sub-component. These profiles can be read and analyzed by using 
`RADICAL Analytics (RA) <http://radicalanalytics.readthedocs.io>`_. The 
documentation discusses profiling in terms of RP executions. The same can be
performed for EnTK as well by two steps:

* Set `stype` to 'radical.entk' when creating the RADICAL Analytics session
* Follow the :ref:`state model <dev_docs_state_model>`, :ref:`event model <dev_docs_events>`, and :ref:`sequence diagram <dev_docs_seq_diag>` to determine the EnTK probes to use in profiling.
