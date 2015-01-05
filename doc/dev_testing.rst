Testing
=======

radical.ensemblemd uses the 'Nose' testing framework. It is installed via pip::

    pip install nose

To run the entire test suite, simply run the following command::

    NOSE_VERBOSE=2 nosetests radical.ensemblemd.tests

If you want to run a sub-set of the unit tests, simply pass the unit test
namespace to the ``nosetests`` command. For example, if you want to run only the
tests for the ``Pipeline`` pattern, run::

    easy_install . && NOSE_VERBOSE=2 nosetests radical.ensemblemd.tests.patterns.pipeline

Adding New Tests for a Pattern
------------------------------

You can run the following sub-tests for the individual patterns:

* ``easy_install . && NOSE_VERBOSE=2 nosetests radical.ensemblemd.tests.patterns.pipline``
* ``easy_install . && NOSE_VERBOSE=2 nosetests radical.ensemblemd.tests.patterns.simulation_analysis_loop``
* ``easy_install . && NOSE_VERBOSE=2 nosetests radical.ensemblemd.tests.patterns.all_pairs``
* ``easy_install . && NOSE_VERBOSE=2 nosetests radical.ensemblemd.tests.patterns.replica_exchange``


Add new pattern-specific tests in ``src/radical/ensemblemd/tests/patterns/patternname/``.
