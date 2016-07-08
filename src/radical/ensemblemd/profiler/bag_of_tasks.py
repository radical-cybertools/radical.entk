def pattern_profiler(pattern_overhead_dict, pat_name, pat_num, pat_stages):

	title = "stage,probe,timestamp"
	f1 = open('enmd_pat_{0}_{1}_overhead.csv'.format(pat_name,pat_num),'w')
	f1.write(title + "\n\n")
			
	for i in range(1,pat_stages+1):
		stage = 'stage_{0}'.format(i)
		for key,val in pattern_overhead_dict[stage].items():                        
			probe = key
			timestamp = val
			entry = '{0},{1},{2}\n'.format(stage,probe,timestamp)
			f1.write(entry)

	f1.close()

def exec_profiler(sid, cu_dict, pat_stages):

	import radical.pilot.utils as rpu
	import pandas as pd

	# Use session ID
	profiles 	= rpu.fetch_profiles(sid=sid, tgt='/tmp/')
	profile    	= rpu.combine_profiles (profiles)
	frame      	= rpu.prof2frame(profile)
	sf, pf, uf 	= rpu.split_frame(frame)

	# Some data wrangling
	rpu.add_info(uf)
	rpu.add_states(uf)
	s_frame, p_frame, u_frame = rpu.get_session_frames(sid)


	# Create second dataframe
	sec_df = pd.DataFrame(columns=["uid","stage"])
	row=0

	for i in range(1, pat_stages+1):
					
		stage = 'stage_{0}'.format(i)
		cus = cu_dict[stage]

		for cu in cus:
			entry = [cu.uid, stage.split('_')[1]]
			sec_df.loc[row] = entry
			row+=1

	# Some data unification
	union = pd.merge(u_frame,sec_df,how='inner',on=['uid'])

	# Required CU info to CSV
	# header = [AgentStagingInput,AgentStagingInputPending,AgentStagingOutput,AgentStagingOutputPending,
	# Allocating,AllocatingPending,Canceled,Done,Executing,ExecutingPending,Failed,New,PendingInputStaging,
	# PendingOutputStaging,Scheduling,StagingInput,StagingOutput,Unscheduled,cores,finished,pid,sid,slots,
	# started,uid,iter,stage]
	union.to_csv("execution_profile_{mysession}.csv".format(mysession=sid), sep=",")