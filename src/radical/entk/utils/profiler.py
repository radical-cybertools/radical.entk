__copyright__ = "Copyright 2017-2018, http://radical.rutgers.edu"
__author__ = "Vivek Balasubramanian <vivek.balasubramaniana@rutgers.edu>"
__license__ = "MIT"

import pandas as pd
import argparse
import numpy as np
import pprint
import os

class Profiler(object):

    """
    The Profiler object to read EnTK profiles are produce data frames and 
    durations based on states and events within EnTK.

    :Arguments:
        :src: full path to the folder containing the EnTK profiles        
    """

    def __init__(self, src=None):

        if not src:
            src = './'


        self._src = src

        self._states = dict()
        self._events = dict()

        self._legacy = os.environ.get("RADICAL_ANALYTICS_LEGACY_PROFILES", False)
        if self._legacy:
            self._cols = ['timestamp', 'thread/proc', 'uid', 'state', 'event', 'msg']
        else:
            self._cols = ['timestamp', 'event', 'comp', 'tid', 'uid', 'state', 'msg']

        self._initialize()

    # ------------------------------------------------------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------------------------------------------------------

    def get(self, uid=None, state=None):

        """
        Get a specific object as specified by the 'uid' argument or
        get a list of objects with a specific state as specified by the 'state'
        argument

        :Arguments:
            :uid: Uid of the object whose details are required
            :state: Details of all objects that have the specified state
        """

        if uid and not state:

            data = dict()            

            obj_type = uid.split('.')[0]

            for state, object_collection in self._states_dict.iteritems():
                
                for obj, timestamp in object_collection[obj_type].iteritems():

                    if obj == uid:

                        if 'uid' not in data:
                            data['uid'] = uid

                        data[state] = timestamp

            return data

        elif state and not uid:

            data = dict()

            for object_collection, objs in self._states_dict[state].iteritems():

                for obj, timestamp in objs.iteritems():

                    if 'state' not in data:
                        data['state'] = state

                    data[obj] = timestamp

            return data

        else:

            data = dict()
            obj_type = uid.split('.')[0]

            data['uid'] = uid
            data[state] = self._states_dict[state][obj_type][uid]

            return data


    def duration(self, objects, states=None, events=None):

        """
        Get the duration the list of objects spent between the list of states or 
        events specified

        :Arguments:
            :objects: List of uids of objects across which the duration is to be computed
            :states: The states between which the duration is to be computed
                syntax: [start_state, end_state]
            :events: The events between which the duration is to be computed
                syntax: [start_event, end_event]
        """

        if not isinstance(states, list) and (states is not None):
            states = [states]

        if not isinstance(events, list) and (events is not None):
            events = [events]

        if not isinstance(objects, list):
            objects =[objects]

        if len(objects)==1:
            objects = [objects[0], objects[0]]

        #pprint.pprint(self._states_dict)
        #pprint.pprint(self._events_dict)

        # Check if objects are in accepted format

        # extract info about required objects

        extracted_dict = {}

        '''
        {
            object0: 
                    { 
                        state0: time, 
                        state1: time
                    },
            object1: 
                    { 
                        state0: time, 
                        state1: time
                    },
        }

        '''

        if (states is not None) and events is None:

            for obj in objects:

                extracted_dict[obj] = {}
                extracted_dict[obj][states[0]] = self._states_dict[states[0]][obj.split('.')[2].strip()][obj]
                extracted_dict[obj][states[1]] = self._states_dict[states[1]][obj.split('.')[2].strip()][obj]

            #pprint.pprint(extracted_dict)

            return self._get_Toverlap(extracted_dict, states[0], states[1])


        elif (states is None) and (events is not None):

            for obj in objects:

                extracted_dict[obj] = {}
                extracted_dict[obj][events[0]] = self._events_dict[events[0]][obj.split('.')[2].strip()][obj]
                extracted_dict[obj][events[1]] = self._events_dict[events[1]][obj.split('.')[2].strip()][obj]            

            #pprint.pprint(extracted_dict)

            return self._get_Toverlap(extracted_dict, events[0], events[1])


        elif states is not None and events is not None:

            t1 = None
            t2 = None

            for obj in objects:

                
                if obj.split('.')[0].strip() in ['task', 'stage', 'pipeline']:
                    t1 = float(self._states_dict[states[0]][obj.split('.')[2].strip()][obj])
                else:
                    t2 = float(self._events_dict[events[0]][obj.split('.')[2].strip()][obj])

            if t1>t2:
                return float(t1) - float(t2)
            else:
                return float(t2) - float(t1)


    def list_all_stateful_objects(self):

        """
        List all objects and the details as stored in the profiler
        """

        return self._state_objs

    # ------------------------------------------------------------------------------------------------------------------
    # Private methods
    # ------------------------------------------------------------------------------------------------------------------

    def _initialize(self):
      
        self._get_resource_manager_details()
        self._get_app_manager_details()
        self._get_wfp_details()
        self._get_task_manager_details()

        self._get_state_details()
        self._get_event_details()

        self._get_states_dict()
        self._get_events_dict()


    def _get_resource_manager_details(self):
    
        self._rman_df = pd.read_csv('%s/radical.entk.resource_manager.0000.prof'%self._src,skiprows=[0,1], 
                            names=self._cols)


    def _get_app_manager_details(self):
    
        self._aman_df = pd.read_csv('%s/radical.entk.appmanager.0000.prof'%self._src,skiprows=[0,1], 
                            names=self._cols)
    
    
    def _get_wfp_details(self):
    
        df_obj = pd.read_csv('%s/radical.entk.wfprocessor.0000-obj.prof'%self._src,skiprows=[0,1], 
                            names=self._cols)
    
        df_proc = pd.read_csv('%s/radical.entk.wfprocessor.0000-proc.prof'%self._src,skiprows=[0,1], 
                            names=self._cols)
    
    
        self._wfp_df = pd.concat([df_obj,df_proc]).sort_values(by='timestamp')
    
    
    def _get_task_manager_details(self):
    
        df_obj = pd.read_csv('%s/radical.entk.task_manager.0000-obj.prof'%self._src,skiprows=[0,1], 
                            names = self._cols)
    
        df_proc = pd.read_csv('%s/radical.entk.task_manager.0000-proc.prof'%self._src,skiprows=[0,1], 
                            names = self._cols)
    
    
        self._tman_df = pd.concat([df_obj,df_proc]).sort_values(by='timestamp')
    
    
    def _get_state_details(self):
    
        state_df  = pd.DataFrame(columns=self._cols)
    
        state_df = state_df.append(self._wfp_df[pd.notnull(self._wfp_df['state'])]).sort_values(by='timestamp')
        state_df = state_df.append(self._tman_df[pd.notnull(self._tman_df['state'])]).sort_values(by='timestamp')
    
        self._states_df = state_df.reset_index().drop(['index'], axis=1)
    
    
    
    def _get_event_details(self):
    
        event_df  = pd.DataFrame(columns=self._cols)    

        event_df = event_df.append(self._rman_df[pd.isnull(self._rman_df['state'])]).sort_values(by='timestamp')
        event_df = event_df.append(self._aman_df[pd.isnull(self._aman_df['state'])]).sort_values(by='timestamp')
        event_df = event_df.append(self._wfp_df[pd.isnull(self._wfp_df['state'])]).sort_values(by='timestamp')
        event_df = event_df.append(self._tman_df[pd.isnull(self._tman_df['state'])]).sort_values(by='timestamp')
    
        self._events_df = event_df.reset_index().drop(['index'], axis=1)
    


    def _get_states_dict(self):

        self._states_dict = dict()
        self._state_objs = dict()

        for row in self._states_df.iterrows():

            row = row[1]

            if row['state'] not in self._states_dict:
                self._states_dict[row['state']] = dict()

            obj_type = row['uid'].split('.')[2]
            #obj_name = ''.join([row['uid'].split('.')[2],'.',row['uid'].split('.')[3]])
            obj_name = row['uid']

            if obj_type not in self._states_dict[row['state']]:
                self._states_dict[row['state']][obj_type] = dict()

            self._states_dict[row['state']][obj_type][obj_name] = row['timestamp']


        for row in self._states_df.iterrows():

            row = row[1]

            if 'pipeline' in row['uid']:
                if row['uid'] not in self._state_objs:
                    self._state_objs[row['uid']] = dict()

            elif 'stage' in row['uid']:
                if row['uid'] not in self._state_objs[row['msg']]:
                    self._state_objs[row['msg']][row['uid']] = list()

            elif 'task' in row['uid']:
                for pipeline, stages in self._state_objs.iteritems():
                    for stage, tasks in stages.iteritems():
                        if row['msg'] == stage:
                            if row['uid'] not in tasks:
                                tasks.append(row['uid'])

    def _get_events_dict(self):

        self._events_dict = dict()

        for row in self._events_df.iterrows():

            row = row[1]

            if row['event'] not in self._events_dict:
                self._events_dict[row['event']] = dict()



            if pd.notnull(row['uid']):
                obj_type = row['uid'].split('.')[2]
                #obj_name = ''.join([row['uid'].split('.')[2],'.',row['uid'].split('.')[3]])
                obj_name = row['uid']

                if obj_type not in self._events_dict[row['event']]:
                    self._events_dict[row['event']][obj_type] = dict()

                self._events_dict[row['event']][obj_type][obj_name] = row['timestamp']


        #pprint.pprint(self._events_dict)


    def _get_Toverlap(self, d, start_state, stop_state):
        '''
        Helper function to create the list of lists from which to calculate the
        overlap of the elements of a DataFrame between the two boundaries passed as
        arguments.
        '''

        overlap = 0
        ranges = []

        for obj, states in d.iteritems():
            #print states
            ranges.append([states[start_state], states[stop_state]])



        for crange in self._collapse_ranges(ranges):
            overlap += crange[1] - crange[0]

        return overlap

    def _collapse_ranges(self, ranges):
        """
        given be a set of ranges (as a set of pairs of floats [start, end] with
        'start <= end'. This algorithm will then collapse that set into the
        smallest possible set of ranges which cover the same, but not more nor
        less, of the domain (floats).
    
        We first sort the ranges by their starting point. We then start with the
        range with the smallest starting point [start_1, end_1], and compare to the
        next following range [start_2, end_2], where we now know that start_1 <=
        start_2. We have now two cases:
    
        a) when start_2 <= end_1, then the ranges overlap, and we collapse them
        into range_1: range_1 = [start_1, max[end_1, end_2]
    
        b) when start_2 > end_2, then ranges don't overlap. Importantly, none of
        the other later ranges can ever overlap range_1. So we move range_1 to
        the set of final ranges, and restart the algorithm with range_2 being
        the smallest one.
    
        Termination condition is if only one range is left -- it is also moved to
        the list of final ranges then, and that list is returned.
        """

        final = []

        # sort ranges into a copy list
        _ranges = sorted (ranges, key=lambda x: x[0])

        START = 0
        END = 1

        base = _ranges[0] # smallest range

        for _range in _ranges[1:]:

            if _range[START] <= base[END]:

                # ranges overlap -- extend the base
                base[END] = max(base[END], _range[END])

            else:

                # ranges don't overlap -- move base to final, and current _range
                # becomes the new base
                final.append(base)
                base = _range

        # termination: push last base to final
        final.append(base)

        return final


    