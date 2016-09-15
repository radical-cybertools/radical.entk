from placeholders import resolve_placeholder_vars

import os
import radical.pilot as rp
import radical.utils as ru

def get_output_data(kernel, record, cur_pat, cur_iter, cur_stage, cur_task):

	logger = ru.get_logger("radical.entk.output_staging")

	try:

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

				var=resolve_placeholder_vars(record, cur_pat, cur_iter, cur_stage, cur_task, kernel.copy_output_data[i])

				if var is not None:

					if len(var.split('>')) > 1:
						temp = {
								'source': var.split('>')[0].strip(),
								'target': var.split('>')[1].strip(),
								'action': rp.COPY
							}
					else:
						temp = {
								'source': var.split('>')[0].strip(),
								'target': os.path.basename(var.split('>')[0].strip()),
								'action': rp.COPY
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

				var=resolve_placeholder_vars(record, cur_pat, cur_iter, cur_stage, cur_task, kernel.download_output_data[i])

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
					data_out.append(temp)

			if op_list is None:
				op_list = data_out
			else:
				op_list += data_out
		#------------------------------------------------------------------------------------------------------------------

		return op_list

	except Exception, ex:

		if cur_pat != "None":
			logger.error("Input staging failed for iter:{0}, stage:{1}, instance: {2}".format(cur_iter, cur_stage, cur_task))
		else:
			logger.error("Input staging failed for pat:{3}, iter:{0}, stage:{1}, instance: {2}".format(cur_iter, cur_stage, cur_task, cur_pat))