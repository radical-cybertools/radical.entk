import os
import glob
import traceback
import radical.utils as ru

from radical.entk.exceptions import EnTKError
from radical.entk import states as res
from radical.pilot import states as rps

# pylint: disable=protected-access


def get_hostmap(profile):
    '''
    We abuse the profile combination to also derive a pilot-host map, which
    will tell us on what exact host each pilot has been running.  To do so, we
    check for the PMGR_ACTIVE advance event in agent_0.prof, and use the NTP
    sync info to associate a hostname.
    '''
    # FIXME: This should be replaced by proper hostname logging
    #        in `pilot.resource_details`.

    hostmap = dict()  # map pilot IDs to host names
    for entry in profile:
        if entry[ru.EVENT] == 'hostname':
            hostmap[entry[ru.UID]] = entry[ru.MSG]

    return hostmap


def get_hostmap_deprecated(profiles):
    '''
    This method mangles combine_profiles and get_hostmap, and is deprecated.  At
    this point it only returns the hostmap
    '''

    hostmap = dict()  # map pilot IDs to host names
    for pname, prof in profiles.iteritems():

        if not prof:
            continue

        if not prof[0][ru.MSG]:
            continue

        host, ip, _, _, _ = prof[0][ru.MSG].split(':')
        host_id = '%s:%s' % (host, ip)

        for row in prof:

            if 'agent_0.prof' in pname    and \
                    row[ru.EVENT] == 'advance' and \
                    row[ru.STATE] == rps.PMGR_ACTIVE:
                hostmap[row[ru.UID]] = host_id
                break

    return hostmap


def get_session_profile(sid, src=None):

    if not src:
        src = os.getcwd()

    if os.path.exists(src):

        # EnTK profiles are always on localhost
        profiles = glob.glob("%s/%s/*.prof" % (src, sid))

    else:
        raise EnTKError('%s/%s does not exist' % (src, sid))

    if not profiles:
        raise EnTKError('No profiles found at %s' % src)

    try:

        profiles = ru.read_profiles(profiles=profiles, sid=sid)
        prof, acc = ru.combine_profiles(profiles)
        prof = ru.clean_profile(prof,
                                sid=sid,
                                state_final=res.FINAL,
                                state_canceled=res.CANCELED)

        hostmap = get_hostmap(prof)

        if not hostmap:
            # FIXME: legacy host notation - deprecated
            hostmap = get_hostmap_deprecated(profiles)

        return prof, acc, hostmap

    except Exception as ex:

        # Push the exception raised by child functions
        print traceback.format_exc()
        raise EnTKError('Error: %s' % ex)


def write_session_description(amgr):

    desc = dict()

    desc['entities'] = dict()
    desc['entities']['pipeline'] = {
        'state_model': res._pipeline_state_values,
        'state_values': res._pipeline_state_inv,
        'event_model': dict(),
    }

    desc['entities']['stage'] = {
        'state_model': res._stage_state_values,
        'state_values': res._stage_state_inv,
        'event_model': dict(),
    }

    desc['entities']['task'] = {
        'state_model': res._task_state_values,
        'state_values': res._task_state_inv,
        'event_model': dict(),
    }

    desc['entities']['appmanager'] = {
        'state_model': None,
        'state_values': None,
        'event_model': dict(),
    }

    # Adding amgr to the tree
    tree = dict()
    tree[amgr._uid] = {'uid': amgr._uid,
                       'etype': 'appmanager',
                       'cfg': {},
                       'has': ['pipeline',
                               'wfprocessor',
                               'resource_manager',
                               'task_manager'],
                       'children': list()
                      }

    # Adding wfp to the tree
    wfp = amgr._wfp
    tree[amgr._uid]['children'].append(wfp._uid)
    tree[wfp._uid] = {'uid': wfp._uid,
                      'etype': 'wfprocessor',
                      'cfg': {},
                      'has': [],
                      'children': list()
                     }

    # Adding rmgr to the tree
    rmgr = amgr._resource_manager
    tree[amgr._uid]['children'].append(rmgr._uid)
    tree[rmgr._uid] = {'uid': rmgr._uid,
                       'etype': 'resource_manager',
                       'cfg': {},
                       'has': [],
                       'children': list()
                      }

    # Adding tmgr to the tree
    tmgr = amgr._task_manager
    tree[amgr._uid]['children'].append(tmgr._uid)
    tree[tmgr._uid] = {'uid': tmgr._uid,
                       'etype': 'task_manager',
                       'cfg': {},
                       'has': [],
                       'children': list()
                      }

    # Adding pipelines to the tree
    wf = amgr._workflow
    for pipe in wf:
        tree[amgr._uid]['children'].append(pipe.uid)
        tree[pipe.uid] = {'uid': pipe.uid,
                          'etype': 'pipeline',
                          'cfg': {},
                          'has': ['stage'],
                          'children': list()
                          }
        # Adding stages to the tree
        for stage in pipe.stages:
            tree[pipe.uid]['children'].append(stage.uid)
            tree[stage.uid] = {'uid': stage.uid,
                               'etype': 'stage',
                               'cfg': {},
                               'has': ['task'],
                               'children': list()
                               }
            # Adding tasks to the tree
            for task in stage.tasks:
                tree[stage.uid]['children'].append(task.uid)
                tree[task.uid] = {'uid': task.uid,
                                  'etype': 'task',
                                  'cfg': {},
                                  'has': [],
                                  'children': list()
                                  }

    desc['tree'] = tree
    desc['config'] = dict()

    ru.write_json(desc, '%s/radical.entk.%s.json' % (amgr.sid, amgr.sid))


def get_session_description(sid, src=None):

    if not src:
        src = os.getcwd()

    if os.path.exists(src):

        # EnTK profiles are always on localhost
        desc = ru.read_json("%s/%s/radical.entk.%s.json" % (src, sid, sid))

    else:
        raise EnTKError('%s/%s does not exist' % (src, sid))

    return desc


def write_workflow(workflow, uid, workflow_fout='entk_workflow', fwrite=True):

    try:
        os.mkdir(uid)
    except:
        pass

    data = list()
    if os.path.isfile('%s/%s.json' % (uid, workflow_fout)):
        data = ru.read_json('%s/%s.json' % (uid, workflow_fout))

    stack = ru.stack()
    data.append({'stack': stack})

    for pipe in workflow:

        p = dict()
        p['uid'] = pipe.uid
        p['name'] = pipe.name
        p['state_history'] = pipe.state_history
        p['stages'] = list()

        for stage in pipe.stages:

            s = dict()
            s['uid'] = stage.uid
            s['name'] = stage.name
            s['state_history'] = stage.state_history
            s['tasks'] = list()

            for task in stage.tasks:
                s['tasks'].append(task.to_dict())

            p['stages'].append(s)

        data.append(p)

    if fwrite:
        ru.write_json(data, '%s/%s.json' % (uid, workflow_fout))
        return 0

    return data

# pylint: disable=protected-access
