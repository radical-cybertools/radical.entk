def resolve_placeholder_vars(record, cur_iter, cur_stage, cur_task, path):


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

	elif len(placeholder.split('_'))==6:
		ref_iter		= int(placeholder.split('_')[1])
		ref_stage 	= int(placeholder.split('_')[3])
		ref_task 	= int(placeholder.split('_')[5])

	elif len(placeholder.split('_'))==4:

		ref_iter		= cur_iter
		ref_stage 	= int(placeholder.split('_')[1])
		ref_task	= int(placeholder.split('_')[3])

	elif len(placeholder.split('_'))==2:

		ref_iter		= cur_iter
		ref_stage	= int(placeholder.split('_')[1])
		ref_task	= cur_task

	try:
		return path.replace(placeholder, record["iter_{0}".format(ref_iter)]["stage_{0}".format(ref_stage)]["instace_{0}".format(ref_task)]["path"])
	except Exception, ex:
		print "Please check placeholders used, error: {0}".format(ex)