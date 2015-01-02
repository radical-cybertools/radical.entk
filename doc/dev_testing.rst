Testing
=======

radical.ensemblemd uses the 'Nose' testing framework. It is installed via pip::

    pip install nose

To run the entire test suite, simply run the following command::

    NOSE_VERBOSE=2 nosetests radical.ensemblemd.tests

If you want to run a sub-set of the unit tests, simply pass the unit test
namespace to the ``nosetests`` command. For example, if you want to run only the
tests for the ``Pipeline`` pattern, run::

    NOSE_VERBOSE=2 nosetests radical.ensemblemd
