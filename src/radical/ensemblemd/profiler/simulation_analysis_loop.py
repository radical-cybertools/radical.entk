def pattern_profiler(pattern_overhead_dict, pat_name, pat_num, pat_iters):
	
	title = "iteration,step,kernel,probe,timestamp"
	f1 = open('enmd_pat_{0}_{1}_overhead.csv'.format(pat_name,pat_num),'w')
	f1.write(title + "\n\n")
	iter = 'None'
	step = 'pre_loop'
	kern = 'None'

	if 'pre_loop' in pattern_overhead_dict:
		for key,val in pattern_overhead_dict['preloop'].items():
			probe = key
			timestamp = val
			entry = '{0},{1},{2},{3},{4}\n'.format(iter,step,kern,probe,timestamp)
			f1.write(entry)

			
	for i in range(1,pat_iters+1):
		iter = 'iter_{0}'.format(i)
		for key1,val1 in pattern_overhead_dict[iter].items():
			step = key1
			for key2,val2 in val1.items():
				kern = key2
				for key3,val3 in val2.items():
					probe = key3
					timestamp = val3
					entry = '{0},{1},{2},{3},{4}\n'.format(
						iter.split('_')[1],
						step,
						kern,
						probe,
						timestamp)
	                                
					f1.write(entry)
	f1.close()

def exec_profiler(sid, cu_dict, pat_iters):

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
	sec_df = pd.DataFrame(columns=["uid","iter","step"])
	row=0

	if 'pre_loop' in cu_dict:
		cu = cu_dict['pre_loop']
		entry = [cu.uid, 0, 'pre_loop']
		sec_df.loc[row] = entry
		row+=1

	for i in range(1,pat_iters+1):
		iter = 'iter_{0}'.format(i)
		for key,val in cu_dict[iter].items():
			step = key
			cus = val

			if step == 'sim':
				for cu in cus:
					entry = [cu.uid, iter.split('_')[1], 'sim']
					sec_df.loc[row] = entry
					row+=1

			elif step == 'ana':
				for cu in cus:
					entry = [cu.uid, iter.split('_')[1], 'ana']
					sec_df.loc[row] = entry
					row+=1

	sec_df.to_csv("second_df.csv",sep=",")


	# Some data unification
	union = pd.merge(u_frame,sec_df,how='inner',on=['uid'])

	# Remove second df
	os.remove('second_df.csv')

	# Required CU info to CSV
	# header = [AgentStagingInput,AgentStagingInputPending,AgentStagingOutput,AgentStagingOutputPending,
		# Allocating,AllocatingPending,Canceled,Done,Executing,ExecutingPending,Failed,New,PendingInputStaging,
		# PendingOutputStaging,Scheduling,StagingInput,StagingOutput,Unscheduled,cores,finished,pid,sid,slots,
		# started,uid,iter,step]
	union.to_csv("execution_profile_{mysession}.csv".format(mysession=sid), sep=",")
	## ------------------------------------------------------------------------------------------------------------------------