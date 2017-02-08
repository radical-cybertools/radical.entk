__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2017, http://radical.rutgers.edu"
__license__   = "MIT"

class EnTKError(Exception):
    """EnTKError is the base exception thrown by the Ensemble Toolkit"""
    def __init__ (self, msg):
        super(EnTKError, self).__init__ (msg)


class TypeError(EnTKError):
    """TypeError is thrown if a parameter of a wrong type is passed to a method or function."""

    def __init__ (self, expected_type, actual_type):
        msg = "Expected (base) type %s, but got %s."%(
            str(expected_type), 
            str(actual_type)
            )
        super(TypeError, self).__init__ (msg)


class ValueError(EnTKError):

    def __init__(self, expected_value, actual_value):
        if type(expected_value) != list:
            msg = "Expected value %s, but got %s."%(
                str(expected_value), 
                str(actual_value)
                )
        else:
            text=''
            for item in expected_value:
                text += str(item)

            msg = "Expected values %s, but got %s."%(
                str(text), 
                str(actual_value)
                )

        super(ValueError, self).__init__ (msg)

class ExistsError(EnTKError):
    """TypeError is thrown if a parameter of a wrong type is passed to a method or function."""

    def __init__ (self, item, parent):
        msg = "Object %s already exists in %s."%(
            str(item), 
            str(parent)
            )
        super(ExistsError, self).__init__ (msg)

class MatchError(EnTKError):
    """MatchError is thrown if two parameters are not equal."""

    def __init__ (self, par1, par2):
        msg = "%s does not match %s."%(
            str(par1), 
            str(par2)
            )
        super(MatchError, self).__init__ (msg)

class ArgumentError(EnTKError):
    """A BadArgumentError is thrown if a wrong set of arguments were passed 
    to a kernel.
    """
    def __init__ (self, kernel_name, message, valid_arguments_set):
        msg = "Invalid argument(s) for kernel '%s': %s. Valid arguments are %s."%(
            kernel_name,
            message,
            valid_arguments_set
            )
        super(ArgumentError, self).__init__ (msg)


class NotImplementedError(EnTKError):
    """NotImplementedError is thrown if a class method or function is not 
    implemented."""

    def __init__ (self, method_name, class_name):
        msg = "Method %s() missing implementation in %s."%(
            method_name, 
            class_name
            )
        super(NotImplementedError, self).__init__ (msg)


class NoKernelConfigurationError(EnTKError):
    """NoKernelConfigurationError is thrown if no kernel configuration could
    be found for the provided resource key.
    """
    def __init__ (self, kernel_name, resource_key):
        msg = "Kernel '%s' doesn not have a configuration entry for resource key '%s'."%(kernel_name, resource_key)
        super(NoKernelConfigurationError, self).__init__ (msg)