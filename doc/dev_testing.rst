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

You can run a subset of tests for the individual patterns:

* ``easy_install . && NOSE_VERBOSE=2 nosetests radical.ensemblemd.tests.patterns.pipline``
* ``easy_install . && NOSE_VERBOSE=2 nosetests radical.ensemblemd.tests.patterns.simulation_analysis_loop``
* ``easy_install . && NOSE_VERBOSE=2 nosetests radical.ensemblemd.tests.patterns.all_pairs``
* ``easy_install . && NOSE_VERBOSE=2 nosetests radical.ensemblemd.tests.patterns.replica_exchange``


Adding a New Test for an Issue
------------------------------

If you are addressing an issue that was reported via the GitHub issue tracker,
add the tests to the ``issue_tests.py`` file in the respective test directory
following this simple template::

    #-------------------------------------------------------------------------
    #
    def test__issue_NUMBER(self):
        """ Issue https://github.com/radical-cybertools/radical.ensemblemd/issues/NUMBER
        """
        # ADD THE TEST CASE HERE
        #

You can run the issue tests isolated::

    easy_install . && NOSE_VERBOSE=2 nosetests radical.ensemblemd.tests.patterns.PATTERN.issue_tests
