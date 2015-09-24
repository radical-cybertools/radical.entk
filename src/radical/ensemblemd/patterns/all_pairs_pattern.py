#!/usr/bin/env python

"""This module defines and implements the AllPairs class.
"""

__author__    = "Ioannis Paraskevakos <i.paraskev@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"


from radical.ensemblemd.exceptions import NotImplementedError
from radical.ensemblemd.execution_pattern import ExecutionPattern

PATTERN_NAME = "AllPairs"


# ------------------------------------------------------------------------------
#
class AllPairs(ExecutionPattern):
    """ The All Pairs Pattern.

    """
    #---------------------------------------------------------------------------
    #
    def __init__(self, set1elements, windowsize1=1, set2elements=None, windowsize2=None):
        """Creates a new AllPairs object.

        **Arguments:**

            * **set1elements** ['list']
              The elements of the first set in which All Pairs pattern will be applied.

            * **windowsize1** ['int']
              The Window size for the elements if the first set. Must dividor of the
              set's size. Default value is 1.

            * **set2elements** ['list']
              The elements of the first set in which All Pairs pattern will be applied.
              Default Value is None.

            * **windowsize2** ['int']
              The Window size for the elements of the second set. Must dividor of the
              set's size. Default Value is None.

        **Attributes:**

            * **permutations** [`int`]
              The setsize parameter determines the size of the set where all possible
              permutations result to the same simulation with different parameters. The
              number of elements in the set is defined as the size
        """
        self._set1elements = set1elements
        self._set2elements = set2elements
        self._windowsize1  = windowsize1
        self._windowsize2  = windowsize2
        if set2elements == None :
            self._permutations = len(self._set1elements)*(len(self._set1elements)-1)/2
        else:
            self._permutations = len(self._set1elements)*len(self._set1elements)

        super(AllPairs, self).__init__()

    #---------------------------------------------------------------------------
    #
    @property
    def name(self):
        """Returns the name of the pattern.
        """
        return PATTERN_NAME

        
    #---------------------------------------------------------------------------
    #
    @property
    def permutations(self):
        """Returns the number of permutations.
        """
        return self._permutations

    #---------------------------------------------------------------------------
    #
    #@property
    def set1_elements(self):
        """Returns the list with the elements of the first set.
        """
        return self._set1elements

    #---------------------------------------------------------------------------
    #
    #@property
    def set2_elements(self):
        """Returns the list with the elements of the second set.
        """
        return self._set2elements

    #---------------------------------------------------------------------------
    #
    def set1element_initialization(self, element):
        """This method returns a :class:`radical.ensemblemd.Kernel` object
           and is executed once before comparison for all the elements in the first
           set

        **Arguments:**

            * **element** [`int`]
              The element parameter is a positive integer and references to an
              element of the set

        **Returns:**

            Implementations of this method **must** return a
            :class:`radical.ensemblemd.Kernel` object. An exception is thrown otherwise.

        """
        raise NotImplementedError(
            method_name="set1element_initialization",
            class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def set2element_initialization(self, element):
        """This method returns a :class:`radical.ensemblemd.Kernel` object
           and is executed once before comparison for all the elements in the
           second set

        **Arguments:**

            * **element** [`int`]
              The element parameter is a positive integer and references to an
              element of the set

        **Returns:**

            Implementations of this method **must** return a
            :class:`radical.ensemblemd.Kernel` object. An exception is thrown otherwise.

        """
        raise NotImplementedError(
            method_name="set2element_initialization",
            class_name=type(self))

    #---------------------------------------------------------------------------
    #
    def element_comparison(self, elements1, elements2):
        """This method returns a :class:`radical.ensemblemd.Kernel` object

        **Arguments:**

            * **elements1** [`list`]
              The first list of elements from the set used for the comparison

            * **elements2** [`list`]
              The second list of elements from the set used for the comparison


        **Returns:**

            Implementations of this method **must** return a list
            :class:`radical.ensemblemd.Kernel` object. An exception is thrown otherwise.

        """
        raise NotImplementedError(
          method_name="element_comparison",
          class_name=type(self))
