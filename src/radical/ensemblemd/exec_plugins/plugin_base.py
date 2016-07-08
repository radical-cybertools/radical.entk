#!/usr/bin/env python

"""TODO: Docstring.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"


""" the adaptor base class. """

import radical.utils         as ru
import radical.utils.config  as ruc

from radical.ensemblemd.exceptions import NotImplementedError


# ------------------------------------------------------------------------------
# plugin base class
#
class PluginBase(object):

	__metaclass__ = ru.Singleton

	# --------------------------------------------------------------------------
	#
	def __init__ (self, adaptor_info, adaptor_options=[]) :

		self._info    = adaptor_info
		self._opts    = adaptor_options
		self._name    = adaptor_info['name']

		self._lock    = ru.RLock      (self._name)
		self._logger  = ru.get_logger ('radical.entk.{0}'.format(self._name))

		self._reporter = ru.LogReporter(name='radical.entk.{0}'.format(self._name))

		# has_enabled = False
		# for option in self._opts :
		#     if option['name'] == 'enabled' :
		#         has_enabled = True

		# if not has_enabled :
		#     # *every* adaptor needs an 'enabled' option!
		#     self._opts.append ({
		#         'category'         : self._name,
		#         'name'             : 'enabled',
		#         'type'             : bool,
		#         'default'          : True,
		#         'valid_options'    : [True, False],
		#         'documentation'    : "Enable / disable loading of the adaptor",
		#         'env_variable'     : None
		#         }
		#     )

	# --------------------------------------------------------------------------
	#
	# if sanity_check() is commented out here, then we will only load adaptors
	# which implement the method themselves.
	#
	def sanity_check (self) :
		""" This method can be overloaded by adaptors to check runtime
			conditions on adaptor load time.  The adaptor should raise an
			exception if it will not be able to function properly in the given
			environment, e.g. due to missing dependencies etc.
		"""
		raise NotImplementedError(
		  method_name="get_name",
		  class_name=type(self))

	# --------------------------------------------------------------------------
	#
	def register(self) :
		""" Adaptor registration function. The engine calls this during startup
			to retrieve the adaptor information.
		"""

		return self._info

	# --------------------------------------------------------------------------
	#
	def get_name (self) :
		return self._name

	# --------------------------------------------------------------------------
	#
	def get_info (self) :
		return self._info

	# --------------------------------------------------------------------------
	#
	def get_logger(self):
		return self._logger

	# --------------------------------------------------------------------------
	#
	def verify_pattern(self, pattern, resource):
		"""Verify the pattern.
		"""
		raise NotImplementedError(
		  method_name="verify_pattern",
		  class_name=type(self))

	# --------------------------------------------------------------------------
	#
	def execute_pattern(self, pattern, resource):
		"""Execute the pattern.
		"""
		raise NotImplementedError(
		  method_name="execute_pattern",
		  class_name=type(self))
