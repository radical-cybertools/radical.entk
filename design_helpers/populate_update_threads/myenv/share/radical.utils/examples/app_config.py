
import os
import pprint

import radical.pilot as rp
import radical.utils as ru

# ------------------------------------------------------------------------------
# a default config, which defines:
#   - the structure / keys of the config settings
#   - the default values for the keys
app_config = {
    'log_level'    : 0,
    'scheduler'    : rp.SCHED_ROUND_ROBIN,
    'resources'    : ['tutorial.radical.org', 'localhost'],
    'resource_cfg' :
    {
        'localhost' :
        {
            'needs_account' : False,
            'supports_mpi'  : False,
            'username'      : None, 
            'account'       : None
        },
        'tutorial.radical.org' :
        {
            'needs_account' : False,
            'supports_mpi'  : True,
            'username'      : 'tut_007', 
            'account'       : None
        },
        'sierra.futuregrid.org' :
        {
            'needs_account' : False,
            'supports_mpi'  : True,
            'username'      : None, 
            'account'       : None
        },
        'india.futuregrid.org' :
        {
            'needs_account' : False,
            'supports_mpi'  : True,
            'username'      : None, 
            'account'       : None 
        },
        'stampede.tacc.utexas.edu' :
        {
            'needs_account' : True,
            'supports_mpi'  : True,
            'username'      : None, 
            'account'       : None
        }
    }
}

# ------------------------------------------------------------------------------
# location of the user config
# the config could contain:
# 
# {
#     "scheduler"    : "rp.SCHED_BACKFILLING",
#     "resources"    : ["india.furturegrid.org", "sierra.futuregrid.org"],
#     "resource_cfg" :
#     {
#         "*.futuregrid.org" :
#         {
#             "username"      : "merzky"
#         }
#     }
# }
USER_CONFIG_PATH = os.environ.get ('HOME', '/tmp') + '/.my_app.cfg' 

# load the user config, and merge it with the default config
user_config = ru.read_json_str (USER_CONFIG_PATH)


# merge the user config into the app config, so that the user config keys are
# applied where appropriate
ru.dict_merge (app_config, user_config, policy='overwrite', wildcards=True)


# lets see what we got
pprint.pprint (app_config)


# this should result in :
#
# {
#     'log_level'   : 0,
#     'scheduler'   : 'rp.SCHED_BACKFILLING',
#     'resources'   : ['india.furturegrid.org', 'sierra.futuregrid.org'],
#     'resource_cfg': 
#     {
#         '*.futuregrid.org': 
#         {
#             'username'     : 'merzky'
#         },
#         'india.futuregrid.org': 
#         {
#             'account'      : None,
#             'needs_account': False,
#             'supports_mpi' : True,
#             'username'     : 'merzky'
#         },
#         'localhost': 
#         {
#             'account'      : None,
#             'needs_account': False,
#             'supports_mpi' : False,
#             'username'     : None
#         },
#         'sierra.futuregrid.org': 
#         {
#             'account'      : None,
#             'needs_account': False,
#             'supports_mpi' : True,
#             'username'     : 'merzky'
#         },
#         'stampede.tacc.utexas.edu': 
#         {
#             'account'      : None,
#             'needs_account': True,
#             'supports_mpi' : True,
#             'username'     : None
#         },
#         'tutorial.radical.org': 
#         {
#             'account'      : None,
#             'needs_account': False,
#             'supports_mpi' : True,
#             'username'     : 'tut_007'
#         }
#     }
# }


