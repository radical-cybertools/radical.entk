.. _faq:

**************************
Frequently Asked Questions
**************************

How can I create a custom kernel?
======================
Creating custom kernels is discussed here in :ref:`writing_kernels`
 

Why do I need a MongoDB to run EnsembleMD ?
===============================
A MongoDB instance is used for task coordination and stores the information about the states of each of the tasks.


What are all the remote machines that EnsembleMD currently supports?
==============================================
Ensemble MD relies on `RADICAL Pilot <http://radicalpilot.readthedocs.org/en/latest/>`_ to support execution on different machines. A list of machines supported is provided in :ref:`chapter_resources` and a method to add new machines (not in the pre-configured list) is provided in :ref:`custom_res`.


My Ensemble MD script fails with error X. How do I report the error ?
=========================================
If you encounter an error in any of the examples in the document or in a script that you created, you can report it to the developers. In order to do this, please re-run your script with two environment variables **RADICAL_ENMD_VERBOSE=debug** and **RADICAL_PILOT_VERBOSE=debug**. This would create a ton of messages that you can capture in a file (preferred) or copy on to your clipboard. For example, 

::

	RADICAL_ENMD_VERBOSE=debug RADICAL_PILOT_VERBOSE=debug python get_started.py

* Using this, you can now create a ticket using the `bug tracker <https://github.com/radical-cybertools/radical.ensemblemd/issues>`_. Click on the green **New Issue** button, give it a title, attach the file (or paste the contents in text field), add any detail that you can give us and Submit.
* You can also report the same to the `mailing list <ensemble-toolkit-users@googlegroups.com>`_.


Can I interrupt my script to terminate gracefully ?
==============================
Yes. You can gracefully terminate the executing script by pressing **Ctrl+C**. Once you do this, please wait for a few seconds as it may take time to cancel the job running on the target machine and cleanup other components.