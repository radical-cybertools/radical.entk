__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


class PluginBase(object):

	def __init__(self, plugin_info = None):

		self._plugin_info = plugin_info
		self._resources = list()

	
	
