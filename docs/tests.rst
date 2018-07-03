.. _tests:


******************************
Unit tests & Integration tests
******************************

Following are the unit tests and functional tests currently covered in 
Ensemble Toolkit.

All tests are run on Travis CI. The current build status:

.. image:: https://travis-ci.org/vivek-bala/radical.entk.svg?branch=arch%2Fv0.6
    :target: https://travis-ci.org/vivek-bala/radical.entk

Click the above to look at the test logs. The following are tests are performed
at every commit in every branch.

Component tests
===============

.. automodule:: tests.test_component.test_amgr
    :members:
    :undoc-members:
    :noindex:
    :exclude-members: func_for_synchronizer_test

.. automodule:: tests.test_component.test_modules
    :members:
    :undoc-members:
    :noindex:

.. automodule:: tests.test_component.test_pipeline
    :members:
    :undoc-members:
    :noindex:

.. automodule:: tests.test_component.test_rmgr
    :members:
    :undoc-members:
    :noindex:

.. automodule:: tests.test_component.test_stage
    :members:
    :undoc-members:
    :noindex:

.. automodule:: tests.test_component.test_task
    :members:
    :undoc-members:
    :noindex:

.. automodule:: tests.test_component.test_tmgr
    :members:
    :undoc-members:
    :noindex:
    :exclude-members: func_for_dummy_tmgr_test, func_for_heartbeat_test, 

.. automodule:: tests.test_component.test_tmgr_rp_utils
    :members:
    :undoc-members:
    :noindex:

.. automodule:: tests.test_component.test_wfp
    :members:
    :undoc-members:
    :noindex:
    :exclude-members: func_for_enqueue_test, func_for_dequeue_test


Integration test
================

.. automodule:: tests.test_integration.test_integration_local
    :members: test_integration_local
    :undoc-members: test_integration_local
    :noindex:

.. automodule:: tests.test_integration.test_post_exec
    :members: test_stage_post_exec
    :undoc-members: test_stage_post_exec
    :noindex:


Issue test
==========

.. automodule:: tests.test_issues.test_issue_199
    :members: test_issue_199
    :undoc-members: test_issue_199

.. automodule:: tests.test_issues.test_issue_214
    :members: test_issue_214
    :undoc-members: test_issue_214

.. automodule:: tests.test_issues.test_issue_236
    :members: test_issue_236
    :undoc-members: test_issue_236

.. automodule:: tests.test_issues.test_issue_239
    :members: test_issue_239
    :undoc-members: test_issue_239

.. automodule:: tests.test_issues.test_issue_26
    :members: test_issue_26
    :undoc-members: test_issue_26


Utils test
==========

.. automodule:: tests.test_utils.test_init_transition
    :members: test_utils_sync_with_master
    :undoc-members: test_utils_sync_with_master

.. automodule:: tests.test_utils.test_prof_utils
    :members: test_write_workflow, test_get_session_description, test_get_session_profile
    :undoc-members: test_write_workflow, test_get_session_description, test_get_session_profile


.. automodule:: tests.test_utils.test_sync_with_master
    :members: test_utils_sync_with_master
    :undoc-members: test_utils_sync_with_master
    