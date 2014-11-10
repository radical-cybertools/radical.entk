#!/usr/bin/env python

"""This module defines and implements the AllPairsPattern class.
"""

__author__    = "Ioannis Paraskevakos <i.paraskev@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"


from radical.ensemblemd.exceptions import NotImplementedError
from radical.ensemblemd.execution_pattern import ExecutionPattern

PATTERN_NAME = "AllPairsPattern"


# ------------------------------------------------------------------------------
#
class AllPairsPattern(ExecutionPattern):
    """ The All Pairs Pattern.

    """
    #---------------------------------------------------------------------------
    #
    def __init__(self, setelemets):
        """Creates a new AllPairsPattern object.
 
        **Arguments:**
			  
			* **setelemets** ['list']
			  The elements of the set in which All Pairs pattern will be applied. Can
			  be used as identifiers? Need to think about it.
 
        **Attributes:**

            * **setsize** [`int`]
              The setsize parameter determines the size of the set where all possible
			  permutations result to the same simulation with different parameters. The
			  number of elements in the set is defined as the size

            * **_permutations** [`int`]
              The maximum number of permutations for a set of size defined by setsize. May
			  be used as some kind of identifier? Need to think about it.

        """
        self._size = setelements.__len__()
	self._setelements = setelements
        self._permutations = self._size*(self.size-1)/2

        super(AllPairsPattern, self).__init__()
        
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
    def size(self):
        """Returns the size of the set.
        """
        return self._size

    #---------------------------------------------------------------------------
    #
    @property
    def permutations(self):
        """Returns the number of permutations.
        """
        return self._permutations

    #---------------------------------------------------------------------------
    #
    @property
    def elements(self):
        """Returns the list with the elements of the set.
        """
        return self._setelements

    #---------------------------------------------------------------------------
    #
    def element_initialization(self, element):
        """This method returns a :class:`radical.ensemblemd.Kernel` object
	and is executed once before comparison for all the elements in the set

        **Arguments:**

            * **element** [`int`]
              The element parameter is a positive integer and references to an
	      element of the set

        **Returns:**
            Implementations of this method **must** return a 
            :class:`radical.ensemblemd.Kernel` object. An exception is thrown otherwise.

        """
        raise NotImplementedError(
            method_name="element_initialization",
            class_name=type(self))


    #---------------------------------------------------------------------------
    #
    def element_comparison(self, element1, element2):
        """This method returns a :class:`radical.ensemblemd.Kernel` object

        **Arguments:**

		* **element1** [`int`]
		The first element from the set used for the comparison
		
		* **element2**[`int`]
		The second element from the set used for the comparison
			

        **Returns:**

            Implementations of this method **must** return a list 
            :class:`radical.ensemblemd.Kernel` object. An exception is thrown otherwise.

        """
        raise NotImplementedError(
          method_name="element_comparison",
          class_name=type(self))
