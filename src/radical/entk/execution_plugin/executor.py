__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from plugin_base import PluginBase

class ExecutePoE(PluginBase):

	def execute_poe(kernels_list):
		print kernels_list[0].as_dict()