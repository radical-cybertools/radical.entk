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
        "units_stagein_first_started": {
            "abs": t_id_E_abs,
            "rel": t_id_E_rel
        },
        "units_stagein_last_finished": {
            "abs": t_id_L_abs,
            "rel": t_id_L_rel
        },

        "units_stageout_first_started": {
            "abs": t_od_E_abs,
            "rel": t_od_E_rel
        },
        "units_stageout_last_finished": {
            "abs": t_od_L_abs,
            "rel": t_od_L_rel
        },

        "units_exec_first_started": {
            "abs": t_ex_E_abs,
            "rel": t_ex_E_rel
        },
        "units_exec_last_finished": {
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

    cols = ['pattern_entity', 'value_type', 'lower', 'upper']
    df = pd.DataFrame(columns=cols)

    # iterate over the entities
    for entity in profile:
        start = entity['timings']['start_time']['rel']
        stop = entity['timings']['end_time']['rel']
        df2 = pd.DataFrame([[entity['name'], 'pattern_step', start, stop]],columns=cols)
        df = df.append(df2, ignore_index=True )

        start = entity['timings']['units_stagein_first_started']['rel']
        stop = entity['timings']['units_stagein_last_finished']['rel']
        df2 = pd.DataFrame([[entity['name'], 'units_stagein', start, stop]],columns=cols)
        df = df.append(df2, ignore_index=True )

        start = entity['timings']['units_exec_first_started']['rel']
        stop = entity['timings']['units_exec_last_finished']['rel']
        df2 = pd.DataFrame([[entity['name'], 'units_exec', start, stop]],columns=cols)
        df = df.append(df2, ignore_index=True )

        start = entity['timings']['units_stageout_first_started']['rel']
        stop = entity['timings']['units_stageout_last_finished']['rel']
        df2 = pd.DataFrame([[entity['name'], 'units_stageout', start, stop]],columns=cols)
        df = df.append(df2, ignore_index=True )

    return df
