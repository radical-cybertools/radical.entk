from placeholders import resolve_placeholder_vars

import os
import radical.pilot as rp
import radical.utils as ru



def get_input_data(kernel, record, cur_pat, cur_iter, cur_stage, cur_task, nonfatal=False):

	logger = ru.get_logger("radical.entk.input_staging")

	try:
		
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
			
				var=resolve_placeholder_vars(record, cur_pat, cur_iter, cur_stage, cur_task, kernel.upload_input_data[i])

				if var is not None:
			
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


		#------------------------------------------------------------------------------------------------------------------
		# link_input_data

		data_in = []
		if kernel.link_input_data is not None:
		
			if isinstance(kernel.link_input_data,list):
				pass
			else:
				kernel.link_input_data = [kernel.link_input_data]

			for i in range(0,len(kernel.link_input_data)):
			
				var=resolve_placeholder_vars(record, cur_pat, cur_iter, cur_stage, cur_task, kernel.link_input_data[i])
			
				if var is not None:

					if len(var.split('>')) > 1:
						temp = {
								'source': var.split('>')[0].strip(),
								'target': var.split('>')[1].strip(),
								'action': rp.LINK
							}

						if nonfatal == True:
							temp['flags'] = [rp.NON_FATAL]
						
					else:
						temp = {
								'source': var.split('>')[0].strip(),
								'target': os.path.basename(var.split('>')[0].strip()),
								'action': rp.LINK
							}

						if nonfatal == True:
							temp['flags'] = [rp.NON_FATAL]

					data_in.append(temp)

			if ip_list is None:
				ip_list = data_in
			else:
				ip_list += data_in
		#------------------------------------------------------------------------------------------------------------------

		#------------------------------------------------------------------------------------------------------------------
		# copy_input_data
		data_in = []

		if kernel.copy_input_data is not None:
		
			if isinstance(kernel.copy_input_data,list):
				pass
			else:
				kernel.copy_input_data = [kernel.copy_input_data]
		
			for i in range(0,len(kernel.copy_input_data)):

				var=resolve_placeholder_vars(record, cur_pat, cur_iter, cur_stage, cur_task, kernel.copy_input_data[i])

				if var is not None:

					if len(var.split('>')) > 1:
						temp = {
								'source': var.split('>')[0].strip(),
								'target': var.split('>')[1].strip(),
								'action': rp.COPY
							}	

						if nonfatal == True:
							temp['flags'] = [rp.NON_FATAL]

					else:
						temp = {
								'source': var.split('>')[0].strip(),
								'target': os.path.basename(var.split('>')[0].strip()),
								'action': rp.COPY
							}

						if nonfatal == True:
							temp['flags'] = [rp.NON_FATAL]

					data_in.append(temp)

			if ip_list is None:
				ip_list = data_in
			else:
				ip_list += data_in

		#------------------------------------------------------------------------------------------------------------------

		return ip_list


		#------------------------------------------------------------------------------------------------------------------
		# download_output_data
		data_in = []

		if kernel.download_input_data is not None:
			if isinstance(kernel.download_input_data,list):
				pass
			else:
				kernel.download_input_data = [kernel.download_input_data]

			for i in range(0,len(kernel.download_input_data)):
				var=resolve_placeholder_vars(record, cur_pat, cur_iter, cur_stage, cur_task, kernel.download_input_data[i])
			
				if var is not None:

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

	except Exception, ex:

		if cur_pat != "None":
			logger.error("Input staging failed for iter:{0}, stage:{1}, instance: {2}".format(cur_iter, cur_stage, cur_task))
		else:
			logger.error("Input staging failed for pat:{3}, iter:{0}, stage:{1}, instance: {2}".format(cur_iter, cur_stage, cur_task, cur_pat))

		raise