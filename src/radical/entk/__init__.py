
# ------------------------------------------------------------------------------
#
from .pipeline import Pipeline
from .stage    import Stage
from .task     import Task
from .         import states

from .appman.appmanager import AppManager


# ------------------------------------------------------------------------------
#
import warnings
import os                           as _os
import radical.utils                as _ru
import requests                     as req
from packaging.version import parse as parse_version

def custom_formatwarning(msg, *args, **kwargs):
    # ignore everything except the message
    return str(msg) + '\n'

warnings.formatwarning = custom_formatwarning

version_short, version_detail, version_base, version_branch, \
        sdist_name, sdist_path = _ru.get_version(_os.path.dirname(__file__))

version = version_short

r = req.get("https://pypi.org/pypi/radical.entk/json")
versions = r.json()["releases"].keys()
last_version = list(versions)[-1]
if parse_version(version) < parse_version(last_version):
    warnings.warn("WARNING: You are using radical.entk version %s, however version %s is available." % (version, last_version), UserWarning)
# ------------------------------------------------------------------------------
