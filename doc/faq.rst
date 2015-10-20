.. faq:

.. |br| raw:: html

   <br />

**************************
Frequently Asked Questions
**************************

* **Q: How can I create a custom kernel?**
* **A:** Creating custom kernels is discussed here in :ref:`writing_kernels`
|br|
 
* **Q: Why do I need a MongoDB to run EnsembleMD ?**
* **A:** A MongoDB instance is used for task coordination and stores the information about the states of each of the tasks.
|br|

* **Q: What are all the remote machines that EnsembleMD currently supports?**
* **A:** Ensemble MD relies on `RADICAL Pilot <http://radicalpilot.readthedocs.org/en/stable/>`_ to deploy tasks on different machines. A list of machines supported is provided in :ref:`chapter_resources` and a method to add new machines (not in the pre-configured list) is provided in :ref:`custom_res`.
|br|

* **Q: My Ensemble MD script fails with error X. What should I do ?**
* **A:** If you encounter an error in any of the examples in the document or in a script that you created, you can report it to the developers. In order to do this, please re-run your script with two environment variables **RADICAL_ENMD_VERBOSE=debug** and **RADICAL_PILOT_VERBOSE=debug**. This would create a ton of messages that you can capture in a file (preferred) or copy on to your clipboard. Using this, you can now create a ticket `here <https://github.com/radical-cybertools/ExTASY/issues>`_. Click on the green **New Issue** button, give it a title, attach the file (or paste the contents in text field), add any detail that you can give us and Submit. We'll get back to you ! For example, 

::

	RADICAL_ENMD_VERBOSE=debug RADICAL_PILOT_VERBOSE=debug python get_started.py

