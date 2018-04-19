import radical.utils as _ru
import os
from prof_utils import *

_pwd   = os.path.dirname (__file__)
_root  = "%s/.." % _pwd

version_short, version_detail, version_base, version_branch, \
        sdist_name, sdist_path = _ru.get_version([_root, _pwd])
version = version_short