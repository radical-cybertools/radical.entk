#!/usr/bin/env python

"""A collection of various utilities and helper functions.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2015, http://radical.rutgers.edu"
__license__   = "MIT"

#------------------------------------------------------------------------------
#
def extract_timing_info(units):
    """
    This function extracts timing from a set of CUs:

      * t_id_E: start of earliest input data staging operation
      * t_id_L: end of last input data staging operation
      * t_od_E: start of earliest output data staging operation
      * t_od_L: end of last output data staging operation
      * t_ex_E: start of earliest execution
      * t_ex_L: end of last last execution

    Timings are returned both as absolute and as relative measures.
    """
    all_id_start = []; all_od_start = []; all_ex_start = []
    all_id_stop = []; all_od_stop = []; all_ex_stop = []

    t_id_E_abs = None; t_od_E_abs = None; t_ex_E_abs = None
    t_id_L_abs = None; t_od_L_abs = None; t_ex_L_abs = None

    t_id_E_rel = None; t_od_E_rel = None; t_ex_E_rel = None
    t_id_L_rel = None; t_od_L_rel = None; t_ex_L_rel = None

    for unit in units:
        all_ex_start.append(unit.start_time)
        all_ex_stop.append(unit.stop_time)

        for log in unit.log:
            if "All FTW Input Staging Directives done" in log.logentry:
                all_id_stop.append(log.timestamp)

            if "All FTW output staging directives done" in log.logentry:
                all_od_stop.append(log.timestamp)

        for state in unit.state_history:
            if state.state == "StagingInput":
                all_id_start.append(state.timestamp)

            if state.state == "StagingOutput":
                all_od_start.append(state.timestamp)

    # Find the earliest / latest timings from the timing arrays
    if len(all_id_start) > 0:
        t_id_E_abs = min(all_id_start)
    if len(all_id_stop) > 0:
        t_id_L_abs = max(all_id_stop)
    if len(all_od_start) > 0:
        t_od_E_abs = min(all_od_start)
    if len(all_od_stop) > 0:
        t_od_L_abs = max(all_od_stop)
    if len(all_ex_start) > 0:
        t_ex_E_abs = min(all_ex_start)
    if len(all_ex_stop) > 0:
        t_ex_L_abs = max(all_ex_stop)

    # Determine the time 'origin' for relative timings
    if t_id_E_abs is not None:
        t_origin = t_id_E_abs
    else:
        t_origin = t_ex_E_abs

    if len(all_id_start) > 0:
        t_id_E_rel = t_id_E_abs - t_origin
    if len(all_id_stop) > 0:
        t_id_L_rel = t_id_L_abs - t_origin
    if len(all_od_start) > 0:
        t_od_E_rel = t_od_E_abs - t_origin
    if len(all_od_stop) > 0:
        t_od_L_rel = t_od_L_abs - t_origin
    if len(all_ex_start) > 0:
        t_ex_E_rel = t_ex_E_abs - t_origin
    if len(all_ex_stop) > 0:
        t_ex_L_rel = t_ex_L_abs - t_origin

    return {
        "first_data_stagein_started": {
            "abs": t_id_E_abs,
            "rel": t_id_E_rel
        },
        "last_data_stagein_finished": {
            "abs": t_id_L_abs,
            "rel": t_id_L_rel
        },

        "first_data_stageout_started": {
            "abs": t_od_E_abs,
            "rel": t_od_E_rel
        },
        "last_data_stageout_finished": {
            "abs": t_od_L_abs,
            "rel": t_od_L_rel
        },

        "first_execution_started": {
            "abs": t_ex_E_abs,
            "rel": t_ex_E_rel
        },
        "last_execution_finished": {
            "abs": t_ex_L_abs,
            "rel": t_ex_L_rel
        }
    }

#------------------------------------------------------------------------------
#
def dataframes_from_profile_dict(profile):

    try:
        import pandas as pd
    except Exception, ex:
        print ex
        return None

    cols = ['pattern_entity', 'value_type', 'first_started_abs', 'last_finished_abs', 'first_started_rel', 'last_finished_rel']
    df = pd.DataFrame(columns=cols)

    # iterate over the entities
    for entity in profile:
        start_abs = entity['timings']['start_time']['abs']
        end_abs = entity['timings']['end_time']['abs']
        start_rel = entity['timings']['start_time']['rel']
        end_rel = entity['timings']['end_time']['rel']
        df2 = pd.DataFrame([[entity['name'], 'pattern_step', start_abs, end_abs, start_rel, end_rel]],columns=cols)
        df = df.append(df2, ignore_index=True )

        start_abs = entity['timings']['first_data_stagein_started']['abs']
        end_abs   = entity['timings']['last_data_stagein_finished']['abs']
        start_rel = entity['timings']['first_data_stagein_started']['rel']
        end_rel   = entity['timings']['last_data_stagein_finished']['rel']
        df2 = pd.DataFrame([[entity['name'], 'data_stagein', start_abs, end_abs, start_rel, end_rel]],columns=cols)
        df = df.append(df2, ignore_index=True )

        start_abs = entity['timings']['first_execution_started']['abs']
        end_abs   = entity['timings']['last_execution_finished']['abs']
        start_rel = entity['timings']['first_execution_started']['rel']
        end_rel   = entity['timings']['last_execution_finished']['rel']
        df2 = pd.DataFrame([[entity['name'], 'execution', start_abs, end_abs, start_rel, end_rel]],columns=cols)
        df = df.append(df2, ignore_index=True )

        start_abs = entity['timings']['first_data_stageout_started']['abs']
        end_abs   = entity['timings']['last_data_stageout_finished']['abs']
        start_rel = entity['timings']['first_data_stageout_started']['rel']
        end_rel   = entity['timings']['last_data_stageout_finished']['rel']
        df2 = pd.DataFrame([[entity['name'], 'data_stageout', start_abs, end_abs, start_rel, end_rel]],columns=cols)
        df = df.append(df2, ignore_index=True )

    return df
