def resolve_placeholder_vars(record, cur_stage, cur_task, path):


	# No replacement required -- probably hard-coded paths
	if '$' not in path:
		return path

	# Extract placeholder from path
	if len(path.split('>') == 1):
		placeholder = path.split('/')[0]
	else:
		if path.split('>')[0].strip().startswith('$'):
			placeholder = path.split('>')[0].strip().split('/')[0]
		else:
			placeholder = path.split('>')[1].strip().split('/')[0]

	# If placeholder pointing to shared space in sandbox

	if placeholder == "$SHARED":
		return path.replace(placeholder, 'staging://')

	elif len(placeholder.split('_'))==4:
		ref_stage 	= int(placeholder.split('_')[1])
		ref_task 	= int(placeholder.split('_')[3])

	else:
		ref_stage 	= int(placeholder.split('_')[1])
		ref_task	= cur_task

	


def get_input_data(kernel, record, cur_stage, cur_task):

	ip_list = []
	#------------------------------------------------------------------------------------------------------------------
	# upload_input_data
	data_in = []
	if kernel.upload_input_data is not None:
		if isinstance(kernel.upload_input_data,list):
			pass
		else:
			kernel.upload_input_data = [kernel.upload_input_data]

		for i in range(0,len(kernel.upload_input_data)):
			
			var=resolve_placeholder_vars(record, cur_stage, cur_task, kernel.upload_input_data[i])
			
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
			data_in.append(temp)

		if ip_list is None:
			ip_list = data_in
		else:
			ip_list += data_in
