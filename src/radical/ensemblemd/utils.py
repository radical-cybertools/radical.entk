#!/usr/bin/env python

"""A collection of various utilities and helper functions.
"""

__author__    = "Ole Weider <ole.weidner@rutgers.edu>"
__copyright__ = "Copyright 2015, http://radical.rutgers.edu"
__license__   = "MIT"

import datetime
import os
import math

#------------------------------------------------------------------------------
#
def extract_timing_info(units, pattern_start_time_abs, step_start_time_abs, step_end_time_abs, enmd_overhead, num_kerns=1,cores=None):
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

    import numpy as np

    step_execution_time_abs = None
    step_data_movement_time_abs = None

    step_total_time = step_end_time_abs - step_start_time_abs


    exec_time_ave_kern = []
    data_time_ave_kern = []
    exec_time_err_kern = []
    data_time_err_kern = []

    units_per_kern = len(units)/num_kerns

    if (cores is not None) and (cores < units_per_kern):
        generations = units_per_kern/cores
    else:
        generations = 1

    for i in range(0,num_kerns):

        units_list = units[i*units_per_kern:(i+1)*units_per_kern]

        exec_time_ave_gen = []
        data_time_ave_gen = []
        exec_time_err_gen = []
        data_time_err_gen = []

        units_per_gen = len(units_list)/generations #=cores

        for g in range(0,generations):

            units_gen_list = units_list[g*units_per_gen:(g+1)*units_per_gen]

            units_id_dur_list = []
            units_od_dur_list = []
            units_ex_dur_list = []
        
            #Iterate through all the units of the stage/step and extract the timestamps
            for unit in units_gen_list:

                missing_state_ip = False
                missing_state_op = False

                ex_start    = 0
                ex_stop     = 0
                id_start    = 0
                id_stop     = 0
                od_start    = 0
                od_stop     = 0 


                #Execution time:
                if ((unit.start_time is not None) and (unit.stop_time is not None)):
                    ex_start = unit.start_time
                    ex_stop = unit.stop_time

                #Input Staging time + Output Staging time:
                for state in unit.state_history:

                    #Input Staging start time:
                    #To account for missing states, use both states - PendingInputStaging & StagingInput - using flags
                    if state.state == "PendingInputStaging":
                        if state.timestamp is not None:
                            id_start = state.timestamp
                    else:
                        missing_state_ip = True

                    if ((missing_state_ip==True)and(state.state == "StagingInput")):
                        if state.timestamp is not None:
                            id_start = state.timestamp
                        else:
                            continue

                    #Input Staging stop time:
                    if state.state == "Allocating":
                        if state.timestamp is not None:
                            id_stop = state.timestamp


                    #Output Staging start time:
                    #To account for missing states, use both states - PendingAgentOutputStaging & AgentStagingOutput - using flags
                    if state.state == "PendingAgentOutputStaging":
                        if state.timestamp is not None:
                            od_start = state.timestamp
                    else:
                        missing_state_op = True

                    if ((missing_state_op==True)and(state.state == "AgentStagingOutput")):
                        if state.timestamp is not None:
                            od_start = state.timestamp
                        else:
                            continue

                    #Output Staging stop time:
                    if state.state == "Done":
                        if state.timestamp is not None:
                            od_stop = state.timestamp

                #Aggregation per CU
                units_ex_dur_list.append((ex_stop - ex_start).total_seconds())
                units_id_dur_list.append((id_stop - id_start).total_seconds())
                units_od_dur_list.append((od_stop - od_start).total_seconds())

            #Aggregation per generation within kernel
            exec_time_ave_gen.append(np.average(units_ex_dur_list))
            data_time_ave_gen.append(np.average(units_id_dur_list)+np.average(units_od_dur_list))
            
            #Find standard errors
            exec_time_err_gen.append(math.sqrt(np.var(units_ex_dur_list))/len(units_ex_dur_list))
            data_time_err_gen.append(math.sqrt(np.var(units_id_dur_list) + np.var(units_od_dur_list))/len(units_id_dur_list))            

        #Aggregation per kernel within same stage/step
        exec_time_ave_kern.append(sum(exec_time_ave_gen))
        data_time_ave_kern.append(sum(data_time_ave_gen))
        
        #Find standard errors
        temp=0.0
        for t in range(0,generations):
            temp+=exec_time_err_gen[t]*exec_time_err_gen[t]
        temp=math.sqrt(temp)
        exec_time_err_kern.append(temp)

        temp=0.0
        for t in range(0,generations):
            temp+=data_time_err_gen[t]*data_time_err_gen[t]
        temp=math.sqrt(temp)
        data_time_err_kern.append(temp)

    #Aggregation per stage/step
    summ=0
    #new stderr = sqrt(s1^2 + s2^2)
    for i in range(0,num_kerns):
        summ+=exec_time_err_kern[i]*exec_time_err_kern[i]
    err_exec_abs = math.sqrt(summ)
    ave_exec_abs = sum(exec_time_ave_kern)
    
    summ=0
    for i in range(0,num_kerns):
        summ+=data_time_err_kern[i]*data_time_err_kern[i]
    err_data_abs = math.sqrt(summ)
    ave_data_abs = sum(data_time_ave_kern)

    return {
        "execution_time": {
                "average":ave_exec_abs,
                "error": err_exec_abs
                },
        "data_movement_time": {
            "average": ave_data_abs,
            "error": err_data_abs
             },
        "enmd_overhead_pat": enmd_overhead,
        "rp_overhead": ((step_end_time_abs - step_start_time_abs).total_seconds()) - 
                        ave_data_abs - ave_exec_abs - enmd_overhead,
        }
    

#------------------------------------------------------------------------------
#
def dataframes_from_profile_dict(profile):

    try:
        import pandas as pd
    except Exception, ex:
        print ex
        return None

    cols = ['pattern_entity','value_type','average duration', 'error']
    df = pd.DataFrame(columns=cols)

    # iterate over the entities
    for entity in profile:
        enmd_overhead_pat = entity['timings']['enmd_overhead_pat']
        df2 = pd.DataFrame([[entity['name'], 'enmd_overhead_pat',enmd_overhead_pat,0]],columns=cols)
        df = df.append(df2, ignore_index=True )

        rp_overhead = entity['timings']['rp_overhead']
        df2 = pd.DataFrame([[entity['name'], 'rp_overhead',rp_overhead,0]],columns=cols)
        df = df.append(df2, ignore_index=True )


        execution_time_average = entity['timings']['execution_time']['average']
        execution_time_error = entity['timings']['execution_time']['error']
        df2 = pd.DataFrame([[entity['name'], 'execution', execution_time_average,execution_time_error]],columns=cols)
        df = df.append(df2, ignore_index=True )

        data_movement_time_average = entity['timings']['data_movement_time']['average']
        data_movement_time_error = entity['timings']['data_movement_time']['error']
        df2 = pd.DataFrame([[entity['name'], 'data_movement', data_movement_time_average,data_movement_time_error]],columns=cols)
        df = df.append(df2, ignore_index=True )

    return df
