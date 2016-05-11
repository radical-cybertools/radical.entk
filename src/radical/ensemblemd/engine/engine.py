#!/usr/bin/env python

"""TODO: Docstring.
"""

__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from radical.utils import Singleton

from radical.ensemblemd.exceptions import NoKernelPluginError
from radical.ensemblemd.exceptions import NoExecutionPluginError
from radical.ensemblemd.engine.plugin_registry import plugin_registry
from radical.ensemblemd.engine.kernel_registry import kernel_registry

#-------------------------------------------------------------------------------
#
def get_engine():
	return Engine()

#-------------------------------------------------------------------------------
#
class Engine(object):
	"""The engine coordinates plug-in loading and other internal things.
	"""

	__metaclass__ = Singleton

	#---------------------------------------------------------------------------
	#
	def __init__(self):
		"""Creates the Engine instance (singleton).
		"""

		# Initialize the logging
		self._logger = ru.get_logger('radical.entk.Engine')

		# Load execution plug-ins
		self._execution_plugins = list()
		self._load_execution_plugins()

		# Load kernel plug-ins
		self._kernel_plugins = list()
		self._load_kernel_plugins()

	#---------------------------------------------------------------------------
	#
	def _load_kernel_plugins(self):
		"""Loads the kernel plugins.
		"""
		self._logger.info("Loading kernel plug-ins...")

		# attempt to load all registered kernels
		for kernel_module_name in kernel_registry:

			# first, import the module
			kernel_module = None
			try :
				kernel_module = __import__ (kernel_module_name, fromlist=['Kernel'])

			except Exception as e:
				self._logger.warning(" > Skipping kernel plug-in {0}: module loading failed: {1}".format(kernel_module_name, e))
				continue # skip to next adaptor

			# we expect the plugin module to have a 'Kernel' class
			# implemented, which, on calling 'register()', returns
			# a info dict for all implemented plug-ing classes.
			kernel_instance = None
			kernel_info     = None

			try: 
				kernel_class = kernel_module.Kernel
				#kernel_info     = kernel_instance.register()

				self._logger.info(" > Loaded kernel plug-in '{0}' from {1}".format(
					kernel_class.get_name(),
					kernel_module_name))
				self._kernel_plugins.append(kernel_class)

			except Exception as e:
				self._logger.warning (" > Skipping kernel plug-in {0}: loading failed: '{1}'".format(kernel_module_name, e))

	#---------------------------------------------------------------------------
	#
	def add_kernel_plugin(self, kernel_class):
		"""Adds a user-defined kernel-plugin.
		"""
		try: 
			# kernel_class = kernel_module.Kernel
			#kernel_info     = kernel_instance.register()

			self._logger.info("Loaded user-provided kernel plug-in '{0}'.".format(
				kernel_class.get_name()))
			self._kernel_plugins.append(kernel_class)

		except Exception as e:
			self._logger.error ("Error loading kernel plug-in {0}: loading failed: '{1}'".format(kernel_class, e))
			raise

	#---------------------------------------------------------------------------
	#
	def _load_execution_plugins(self):
		"""Loads the execution plugins.
		"""
		self._logger.info("Loading execution plug-ins...")

		# attempt to load all registered plugins
		for plugin_module_name in plugin_registry:

			# first, import the module
			adaptor_module = None
			try :
				adaptor_module = __import__ (plugin_module_name, fromlist=['Plugin'])

			except Exception as e:
				self._logger.warning(" > Skipping execution plug-in {0}: module loading failed: {1}".format(plugin_module_name, e))
				continue # skip to next adaptor

			# we expect the plugin module to have an 'Adaptor' class
			# implemented, which, on calling 'register()', returns
			# a info dict for all implemented adaptor classes.
			plugin_instance = None
			plugin_info     = None

			try: 
				plugin_instance = adaptor_module.Plugin()
				plugin_info     = plugin_instance.register()

				self._logger.info(" > Loaded execution plug-in '{0}' from {1}".format(
					plugin_instance.get_name(),
					plugin_module_name))
				self._execution_plugins.append(plugin_instance)

			except Exception as e:
				self._logger.warning(" > Skipping execution plug-in {0}: loading failed: '{1}'".format(plugin_module_name, e))

	#---------------------------------------------------------------------------
	#
	def get_execution_plugin_for_pattern(self, pattern_name, context_name, plugin_name):
		"""Returns an execution plug-in for a given pattern and context.
		"""
		plugin = None

		for candidate_plugin in self._execution_plugins:
			for_pattern = candidate_plugin.get_info()['pattern']
			for_context = candidate_plugin.get_info()['context_type']
			if (for_pattern == pattern_name) and (for_context == context_name):
				if plugin_name is None:
					plugin = candidate_plugin
					break
				else:
					if candidate_plugin.get_name() == plugin_name:
						plugin = candidate_plugin
						break

		if plugin != None:
			self._logger.info("Selected execution plug-in '{0}' for pattern '{1}' and context type '{2}'.".format(
				plugin.get_name(),
				pattern_name,
				context_name)
			)
			return plugin
		else:
			error = NoExecutionPluginError(
				pattern_name=pattern_name,
				context_name=context_name,
				plugin_name=plugin_name)
			self._logger.error(str(error))
			raise error

	#---------------------------------------------------------------------------
	#
	def get_kernel_plugin(self, kernel_name):
		"""Returns a kernel plug-in for a given name.
		"""
		kernel = None

		for candidate_kernel in self._kernel_plugins:
			if candidate_kernel().get_name() == kernel_name:
				kernel = candidate_kernel
				break

		if kernel != None:
			#self._logger.debug("Selected kernel plug-in '{0}'.".format(kernel.get_name()))
			# Create a new instance of 'kernel' and return it to the caller.
			return kernel()
		else:
			error = NoKernelPluginError(kernel_name=kernel_name)
			self._logger.error(str(error))
			raise error

