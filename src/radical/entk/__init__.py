
# ------------------------------------------------------------------------------
#
from .pipeline import Pipeline
from .stage    import Stage
from .task     import Task
from .         import states

from .appman.appmanager import AppManager


# ------------------------------------------------------------------------------
#
import os            as _os
import radical.utils as _ru

version_short, version_detail, version_base, version_branch, \
        sdist_name, sdist_path = _ru.get_version(_os.path.dirname(__file__))

version = version_short


# ------------------------------------------------------------------------------

