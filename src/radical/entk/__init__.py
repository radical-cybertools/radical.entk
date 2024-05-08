
__copyright__ = "Copyright 2013-2024, http://radical.rutgers.edu"
__license__   = "MIT"


# ------------------------------------------------------------------------------
# we *first* import radical.utils, so that the monkeypatching of the logger has
# a chance to kick in before the logging module is pulled by any other 3rd party
# module, and also to monkeypatch `os.fork()` for the `atfork` functionality
#
import os            as _os
import radical.utils as _ru


# ------------------------------------------------------------------------------
#
from .appman   import AppManager
from .pipeline import Pipeline
from .stage    import Stage
from .task     import Task
from .         import states
from .         import execman


# ------------------------------------------------------------------------------
#
# get version info
#
_mod_root = _os.path.dirname (__file__)

version_short, version_base, version_branch, version_tag, version_detail \
             = _ru.get_version(_mod_root)
version      = version_short
__version__  = version_detail


# ------------------------------------------------------------------------------

