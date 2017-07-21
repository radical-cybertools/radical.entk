import pandas as pd
import argparse
import numpy as np
import pprint

class Profiler(object):

    def __init__(self, src=None):

        if not src:
            src = './'


        self._src = src

        self._states = dict()
        self._events = dict()

        self._initialize()


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
                            names=['timestamp', 'thread/proc', 'uid', 'state', 'event', 'msg']
                        )


    def _get_app_manager_details(self):
    
        self._aman_df = pd.read_csv('%s/radical.entk.appmanager.0000.prof'%self._src,skiprows=[0,1], 
                            names=['timestamp', 'thread/proc', 'uid', 'state', 'event', 'msg']
                        )
    
    
    def _get_wfp_details(self):
    
        df_obj = pd.read_csv('%s/radical.entk.wfprocessor.0000-obj.prof'%self._src,skiprows=[0,1], 
                            names=['timestamp', 'thread/proc', 'uid', 'state', 'event', 'msg']
                        )
    
        df_proc = pd.read_csv('%s/radical.entk.wfprocessor.0000-proc.prof'%self._src,skiprows=[0,1], 
                            names=['timestamp', 'thread/proc', 'uid', 'state', 'event', 'msg']
                        )
    
        self._wfp_df = pd.concat([df_obj,df_proc]).sort_values(by='timestamp')
    
    
    def _get_task_manager_details(self):
    
        df_obj = pd.read_csv('%s/radical.entk.task_manager.0000-obj.prof'%self._src,skiprows=[0,1], 
                            names=['timestamp', 'thread/proc', 'uid', 'state', 'event', 'msg']
                        )
    
        df_proc = pd.read_csv('%s/radical.entk.task_manager.0000-proc.prof'%self._src,skiprows=[0,1], 
                            names=['timestamp', 'thread/proc', 'uid', 'state', 'event', 'msg']
                        )
    
        self._tman_df = pd.concat([df_obj,df_proc]).sort_values(by='timestamp')
    
    
    def _get_state_details(self):
    
        state_df  = pd.DataFrame(columns=['timestamp', 'thread/proc', 'uid', 'state', 'event', 'msg'])
    
        state_df = state_df.append(self._wfp_df[pd.notnull(self._wfp_df['state'])]).sort_values(by='timestamp')
        state_df = state_df.append(self._tman_df[pd.notnull(self._tman_df['state'])]).sort_values(by='timestamp')
    
        self._states_df = state_df.reset_index().drop(['index'], axis=1)
    
    
    
    def _get_event_details(self):
    
        event_df  = pd.DataFrame(columns=['timestamp', 'thread/proc', 'uid', 'state', 'event', 'msg'])    

        event_df = event_df.append(self._rman_df[pd.isnull(self._rman_df['state'])]).sort_values(by='timestamp')
        event_df = event_df.append(self._aman_df[pd.isnull(self._aman_df['state'])]).sort_values(by='timestamp')
        event_df = event_df.append(self._wfp_df[pd.isnull(self._wfp_df['state'])]).sort_values(by='timestamp')
        event_df = event_df.append(self._tman_df[pd.isnull(self._tman_df['state'])]).sort_values(by='timestamp')
    
        self._events_df = event_df.reset_index().drop(['index'], axis=1)
    


    def _get_states_dict(self):

        self._states_dict = dict()

        for row in self._states_df.iterrows():

            row = row[1]

            if row['state'] not in self._states_dict:
                self._states_dict[row['state']] = dict()

            obj_type = row['uid'].split('.')[2]
            obj_name = ''.join([row['uid'].split('.')[2],'.',row['uid'].split('.')[3]])

            if obj_type not in self._states_dict[row['state']]:
                self._states_dict[row['state']][obj_type] = dict()

            self._states_dict[row['state']][obj_type][obj_name] = row['timestamp']

        #pprint.pprint(self._states_dict)



    def _get_events_dict(self):

        self._events_dict = dict()

        for row in self._events_df.iterrows():

            row = row[1]

            if row['event'] not in self._events_dict:
                self._events_dict[row['event']] = dict()



            if pd.notnull(row['uid']):
                obj_type = row['uid'].split('.')[2]
                obj_name = ''.join([row['uid'].split('.')[2],'.',row['uid'].split('.')[3]])

                if obj_type not in self._events_dict[row['event']]:
                    self._events_dict[row['event']][obj_type] = dict()

                self._events_dict[row['event']][obj_type][obj_name] = row['timestamp']


        #pprint.pprint(self._events_dict)

        
    def get(self, uid=None, state=None):

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

        if not isinstance(states, list):
            states = [states]

        if not isinstance(events, list):
            events = [events]

        if len(states) == 2:

            p1 = states[0]
            p2 = states[1]

        elif len(events) == 2:

            p1 = events[0]
            p2 = events[1]

        else:

            if (len(states)==1 and len(events)==1):
                
                p1 = states[0]
                p2 = events[0]                

            else:

                print 'Need at least two values between states, events'


        if not events:

            # Means interested only in states

            t_min = None
            t_max = None

            for obj in objects:

                self._states_dict[p1]



                