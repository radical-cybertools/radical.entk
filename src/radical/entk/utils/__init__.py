
# ------------------------------------------------------------------------------
#
from .prof_utils         import get_hostmap
from .prof_utils         import get_hostmap_deprecated
from .prof_utils         import get_session_profile
from .prof_utils         import write_session_description
from .prof_utils         import get_session_description
from .prof_utils         import write_workflows


# ------------------------------------------------------------------------------
#
import os            as _os
import radical.utils as _ru

_pwd   = _os.path.dirname(__file__)
_root  = "%s/.." % _pwd

version_short, version_detail, version_base, version_branch, \
        sdist_name, sdist_path = _ru.get_version([_root, _pwd])
version = version_short


# ------------------------------------------------------------------------------

