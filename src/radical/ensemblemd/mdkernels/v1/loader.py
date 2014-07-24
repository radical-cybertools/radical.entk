__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"
__author__    = "Ole Weidner <ole.weidner@rutgers.edu>"

import os
import json
import glob

from logger import logger
from radical.utils.singleton import Singleton

# -----------------------------------------------------------------------------
#
class _KernelDict(object):
    """Singleton class to initialize the kernel configuration dictionary.
    """
    __metaclass__ = Singleton

    def __init__(self):
        """Create or get a new instance (singleton).
        """
        # keneldict holds all configuration
        self._kerneldict = dict()

        cwd = os.path.dirname(os.path.realpath(__file__))
        config_files = glob.glob("{0}/configs/*.json".format(cwd))

        try: 
            for f in config_files:
                logger.info("Loading kernel configurations from {0}".format(f))

                # load file into a dictionary
                json_data=open(f)
                data = json.load(json_data)

                kernel_name = data["kernel_name"]
                kernel_cfgs = data["kernel_configs"]
                logger.debug("Found MD kernel '{0}' configurations for {1}".format(kernel_name, kernel_cfgs.keys()))

                self._kerneldict[data["kernel_name"]] = data["kernel_configs"]

        except Exception, ex:
            logger.error("Error loading JSON file: {0}".format(str(ex)))
            raise

    def get(self):
        """Return the kernel dictionary.
        """
        return self._kerneldict

# -----------------------------------------------------------------------------
#
kerneldict = _KernelDict().get()
