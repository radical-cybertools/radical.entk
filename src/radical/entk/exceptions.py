
__copyright__ = "Copyright 2017-2019, http://radical.rutgers.edu"
__author__    = "RADICAL Team <radical@rutgers.edu>"
__license__   = "MIT"


# ------------------------------------------------------------------------------
#
class EnTKError(Exception):
    """
    EnTKError is the base exception raised by Ensemble Toolkit
    """

    def __init__(self, msg):

        super(EnTKError, self).__init__(msg)


# ------------------------------------------------------------------------------
#
class TypeError(EnTKError):
    """
    TypeError is raised if value of a wrong type is passed to a function or
    assigned as an attribute of an object
    """

    def __init__(self, expected_type, actual_type, entity=None):

        if entity:
            msg = "Entity: %s, Expected (base) type(s) %s, but got %s." \
                % (entity, expected_type, actual_type)

        else:
            msg = "Expected (base) type(s) %s, but got %s." \
                % (expected_type, actual_type)

        super(TypeError, self).__init__(msg)


# ------------------------------------------------------------------------------
#
class ValueError(EnTKError):
    """
    ValueError is raised if a value that is unacceptable is passed to a
    function or assigned as an attribute of an object
    """

    def __init__(self, obj, attribute, expected_value, actual_value):

        if type(expected_value) != list:

            msg = "Value for attribute %s of object %s incorrect. " + \
                  "Expected value %s, but got %s." \
                % (obj, attribute, expected_value, actual_value)
        else:

            # AM: I don't understand the purpose of concatinating w/o delimiter.
            # If,  say, `expected_values` is `['foo', 'bar']`, the user will be
            # told that the expected value is `'foobar'`?
            # AM: also, this is shorter:  `text = ''.join(expected_values)`
            text = ''
            for item in expected_value:
                text += str(item)

            msg = "Value for attribute %s of object %s incorrect. " + \
                  "Expected values %s, but got %s." \
                % (obj, attribute, text, actual_value)

        super(ValueError, self).__init__(msg)


# ------------------------------------------------------------------------------
#
class MissingError(EnTKError):
    """
    MissingError is raised when an attribute that is mandatory is left
    unassigned by the user
    """

    def __init__(self, obj, missing_attribute):

        msg = 'Attribute %s in %s undefined' % (missing_attribute, obj)
        super(MissingError, self).__init__(msg)


# ------------------------------------------------------------------------------

