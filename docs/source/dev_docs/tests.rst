.. _tests:


******************************
Unit tests & Integration tests
******************************

All tests are run on a Travis. Tests are run using pytest and test coverage
is measured using `coverage <https://coverage.readthedocs.io/>`_ and reported using `codecov <https://codecov.io>`_ .


Unit tests test the functionality of basic methods and functions
offered by EnTK. We thrive to create and include a new test for every new
feature offered by EnTK.

Integration tests test the correct communication between different EnTK components
and packages and services used by EnTK, such as RADICAL-Pilot.

Writing tests for EnTK requires to follow the `test coding guidelines <https://github.com/radical-cybertools/radical.pilot/wiki/Tests-Coding-Guidelines>`_
of RADICAL. An example can be found `here <https://github.com/radical-cybertools/radical.entk/blob/devel/tests/test_component/test_tproc_rp.py>`_.