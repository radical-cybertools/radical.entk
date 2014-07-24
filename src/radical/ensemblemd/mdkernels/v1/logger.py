__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"
__author__    = "Ole Weidner <ole.weidner@rutgers.edu>"

from logging import Formatter

from radical.utils.singleton import Singleton
import radical.utils.logger as rul

# -----------------------------------------------------------------------------
#
class _MPLogger(object):
    """Singleton class to initialize custom multiprocessing logger.
    """
    __metaclass__ = Singleton

    def __init__(self):
        """Create or get a new logger instance (singleton).
        """
        self._logger = rul.logger.getLogger(name='radical.ensemblemd')
        mp_formatter = Formatter(fmt='%(asctime)s %(name)s: [%(levelname)-8s] %(message)s', 
                                 datefmt='%Y:%m:%d %H:%M:%S')

#        mp_formatter = Formatter(fmt='%(asctime)s %(name)s.%(processName)s: [%(levelname)-8s] %(message)s', 
#                                 datefmt='%Y:%m:%d %H:%M:%S')

        for handler in self._logger.handlers:
            handler.setFormatter(mp_formatter)

        self._logger.info('radical.ensemblemd version: %s' % "DEV")

    def get(self):
        """Return the logger.
        """
        return self._logger

# -----------------------------------------------------------------------------
#
logger = _MPLogger().get()
