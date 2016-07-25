__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


from plugin_base import PluginBase
import radical.pilot as rp
import radical.utils as ru

import saga

_plugin_info = {
			'name': 'eop',
			'type': 'static'
		}

class PluginEoP(object):

	def __init__(self):

		self._executable_workload = None
		self._resource = None
		self._manager = None
		self._monitor = None

		self._logger = ru.get_logger("radical.entk.plugin.eop")
		self._reporter = self._logger.report

		self._tot_fin_tasks=[0]

		self._logger.info("Plugin EoP created")


	def register_resource(self, resource):
		self._resource = resource
		self._logger.info("Registered resource {0} with execution plugin".format(resource))

	def get_resources(self):
		return self._resource


	def set_workload(self, pattern, monitor=None):

		self._executable_workload = pattern
		self._logger.info("New workload assigned to plugin for execution")

		self._monitor = monitor
		if monitor is not None:
			self._logger.info("Monitor for workload assigned")

	def add_manager(self, manager):
		self._manager = manager
		self._logger.debug("Task execution manager (RP-Unit Manager) assigned to execution plugin")


	def execute(self, record):


		def unit_state_cb (unit, state) :

			if state == rp.FAILED:
				self._logger.error("Task with ID {0} failed: STDERR: {1}, STDOUT: {2} LAST LOG: {3}".format(unit.uid, unit.stderr, unit.stdout, unit.log[-1]))
				self._logger.error("Pattern execution FAILED.")
				sys.exit(1)

			if state == rp.DONE:

				try:

					cur_stage = int(unit.name.split('-')[1])
					cur_task = int(unit.name.split('-')[3])
					self._logger.info('Task {0} of stage {1} has finished'.format(cur_task,cur_stage))


					#-----------------------------------------------------------------------
					# Increment tasks list accordingly
					if self.tot_fin_tasks[0] == 0:
						self.tot_fin_tasks[0] = 1

					else:

						self.tot_fin_tasks[cur_stage-1]+=1

						# Check if this is the last task of the stage
						if self.tot_fin_tasks[cur_stage-1] == self._executable_workload.pipeline_size:
							self._logger.info('All tasks in stage {0} has finished'.format(cur_stage))


					cud = create_next_stage_cud(unit)
					if cud is not None:
						launch_next_stage(cud)

				except Exception, ex:
					self._logger.error('Failed to trigger next stage, error: {0}'.format(ex))
					raise


		# Main code
		try:

			self._manager.register_callback(unit_state_cb)

			from staging.input_data import get_input_data
			from staging.output_data import get_output_data

			task_method = self.get_stage(stage=1)

			cus = []
			for task_instance in range(1, self._executable_workload.ensemble_size+1):

				kernel = task_method(task_instance)
				kernel._bind_to_resource(self._resource)
				rbound_kernel = kernel

				self._logger.debug('Creating task {0} of stage 1'.format(task_instance))

				cud = radical.pilot.ComputeUnitDescription()
				cud.name = "stage-1-task-{0}".format(task_instance)

				cud.pre_exec       	= rbound_kernel.pre_exec
				cud.executable     	= rbound_kernel.executable
				cud.arguments      	= rbound_kernel.arguments
				cud.mpi            		= rbound_kernel.uses_mpi
				cud.cores 		= rbound_kernel.cores

				'''
				cud.input_staging  	= get_input_data(rbound_kernel, 
									record, 
									cur_pat = self._executable_workload.name, 
									cur_iter = self._executable_workload.cur_iteration, 
									cur_stage = 1, 
									cur_task= task_instance)

				cud.output_staging 	= get_output_data(rbound_kernel, 
									record, 
									cur_pat = self._executable_workload.name, 
									cur_iter= self._executable_workload.cur_iteration, 
									cur_stage = 1, 
									cur_task= task_instance)
				'''

				cus.append(cud)

			exec_cus = self._manager.submit_units(cus)
			self._logger.info('Submitted all tasks of stage 1')


			#-----------------------------------------------------------------------
			# Create next CU at the end of each CU
			def create_next_stage_cud(unit):
						
				cur_stage = int(unit.name.split('-')[1])+1
				cur_task = int(unit.name.split('-')[3])

				if cur_stage <= num_stages:

					if len(self.tot_fin_tasks) < cur_stage:
						self.tot_fin_tasks.append(0)
						self._logger.info('\nStarting submission of tasks in stage {0}'.format(cur_stage))				
					
					self._logger.debug('Creating task {0} of stage {1}'.format(cur_task,cur_stage))

					task_method = self.get_stage(stage=cur_stage)

					kernel = task_method(cur_task)
					kernel._bind_to_resource(self._resource)
					rbound_kernel = kernel

					cud = radical.pilot.ComputeUnitDescription()
					cud.name = "stage-{0}-task-{1}".format(cur_stage,cur_task)

					cud.pre_exec       	= rbound_kernel.pre_exec
					cud.executable     	= rbound_kernel.executable
					cud.arguments      	= rbound_kernel.arguments
					cud.mpi            		= rbound_kernel.uses_mpi
					cud.cores 		= rbound_kernel.cores

					'''
					cud.input_staging  	= get_input_data(rbound_kernel, 
									record, 
									cur_pat = self._executable_workload.name, 
									cur_iter = self._executable_workload.cur_iteration, 
									cur_stage = cur_stage, 
									cur_task= cur_task)

					cud.output_staging 	= get_output_data(rbound_kernel, 
									record, 
									cur_pat = self._executable_workload.name, 
									cur_iter= self._executable_workload.cur_iteration, 
									cur_stage = cur_stage, 
									cur_task= cur_task)
					'''

					return cud

				else:
					return None

			#-----------------------------------------------------------------------

			#-----------------------------------------------------------------------
			# Launch the CU of the next stage
			def launch_next_stage(cud):

				cur_stage = int(cud.name.split('-')[1])
				cur_task = int(cud.name.split('-')[3])
				self._logger.info('Submitting task {0} of stage {1}'.format(cur_task,cur_stage))
				task = self._manager.submit_units(cud)
			#-----------------------------------------------------------------------

			#-----------------------------------------------------------------------
			# Wait for all tasks to finish
			while(sum(self.tot_fin_tasks)!=(self._executable_workload.pipeline_size*self._executable_workload.ensemble_size)):
				self._manager.wait_units() 

		except Exception, ex:
			self._logger("Execution failed at plugin, error: {0}".format(ex))
			raise