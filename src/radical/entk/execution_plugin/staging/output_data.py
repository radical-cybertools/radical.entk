from placeholders import resolve_placeholder_vars

import os

def get_output_data(kernel, record, cur_iter, cur_stage, cur_task):

	# OUTPUT DATA:
	#------------------------------------------------------------------------------------------------------------------
	# copy_output_data
	op_list = []
	
	data_out = []
	if kernel.copy_output_data is not None:
		if isinstance(kernel.copy_output_data,list):
			pass
		else:
			kernel.copy_output_data = [kernel.copy_output_data]
		for i in range(0,len(kernel.copy_output_data)):
			var=resolve_placeholder_vars(record, cur_iter, cur_stage, cur_task, kernel.copy_output_data[i])
			if len(var.split('>')) > 1:
				temp = {
						'source': var.split('>')[0].strip(),
						'target': var.split('>')[1].strip(),
						'action': radical.pilot.COPY
					}
			else:
				temp = {
						'source': var.split('>')[0].strip(),
						'target': os.path.basename(var.split('>')[0].strip()),
						'action': radical.pilot.COPY
					}
			data_out.append(temp)

		if op_list is None:
			op_list = data_out
		else:
			op_list += data_out
	#------------------------------------------------------------------------------------------------------------------

	#------------------------------------------------------------------------------------------------------------------
	# download_output_data
	data_out = []

	if kernel.download_output_data is not None:
		if isinstance(kernel.download_output_data,list):
			pass
		else:
			kernel.download_output_data = [kernel.download_output_data]

		for i in range(0,len(kernel.download_output_data)):
			var=resolve_placeholder_vars(record, cur_iter, cur_stage, cur_task, kernel.download_output_data[i])
			
			if len(var.split('>')) > 1:
				temp = {
						'source': var.split('>')[0].strip(),
						'target': var.split('>')[1].strip()
					}
			else:
				temp = {
						'source': var.split('>')[0].strip(),
						'target': os.path.basename(var.split('>')[0].strip())
					}
			data_out.append(temp)

		if op_list is None:
			op_list = data_out
		else:
			op_list += data_out
	#------------------------------------------------------------------------------------------------------------------

	return op_list