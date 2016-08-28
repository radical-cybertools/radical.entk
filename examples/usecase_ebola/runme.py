__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.entk import EoP, AppManager, Kernel, ResourceHandle

from echo import echo_kernel
from randval import rand_kernel
from sleep import sleep_kernel

class Test(EoP):

	def __init__(self, ensemble_size, pipeline_size):
		super(Test,self).__init__(ensemble_size, pipeline_size)

	def stage_1(self, instance):
		k1 = Kernel(name="sleep")
		k1.arguments = ["--file=output.txt","--text=simulation","--duration=60"]
		k1.cores = 1

		# File staging
		#k1.upload_input_data = []
		#k1.copy_input_data = []
		#k1.link_input_data = []
		#k1.copy_output_data = []
		#k1.download_output_data = []

		# Define "async" monitor -- is executed every 20 seconds, cancels the first task if running
		m1 = Kernel(name="echo", ktype="monitor")
		m1.timeout = 30
		m1.arguments = ["--file=test.txt","--text=monitor"]
		m1.copy_input_data = ['$STAGE_1_TASK_2/output.txt']
		m1.download_output_data = ['test.txt']
		m1.cancel_tasks = [1]

		return [k1,m1]


	def stage_2(self, instance):
		k1 = Kernel(name="echo")
		k1.arguments = ["--file=output.txt","--text=build_systems"]
		k1.cores = 1

		# File staging
		#k1.upload_input_data = []
		#k1.copy_input_data = []
		#k1.link_input_data = []
		#k1.copy_output_data = []
		#k1.download_output_data = []
		return k1

	def stage_3(self, instance):
		k1 = Kernel(name="echo")
		k1.arguments = ["--file=output.txt","--text=equilibrate"]
		k1.cores = 1

		# File staging
		#k1.upload_input_data = []
		#k1.copy_input_data = []
		#k1.link_input_data = []
		#k1.copy_output_data = []
		#k1.download_output_data = []

		return k1

	def stage_4(self, instance):
		k1 = Kernel(name="echo")
		k1.arguments = ["--file=output.txt","--text=MD"]
		k1.cores = 1

		# File staging
		#k1.upload_input_data = []
		#k1.copy_input_data = []
		#k1.link_input_data = []
		#k1.copy_output_data = []
		#k1.download_output_data = []
		return k1


	def stage_5(self, instance):
		k1 = Kernel(name="randval")
		k1.arguments = ["--upperlimit=5"]
		k1.cores = 1

		# File staging
		#k1.upload_input_data = []
		#k1.copy_input_data = []
		#k1.link_input_data = []
		#k1.copy_output_data = []
		#k1.download_output_data = []
		return k1

	def branch_5(self, instance):

		flag = self.get_output(stage=5, instance=instance)
		print 'Output of stage:5, instance:{0} = {1}'.format(instance,flag)
		if int(flag) > 3:
			self.set_next_stage(1)
			print 'Restarting instance {0}'.format(instance)
		else:
			pass


	def stage_6(self, instance):

		k1 = Kernel(name="randval")
		k1.arguments = ["--upperlimit=5"]
		k1.cores = 1

		# File staging
		#k1.upload_input_data = []
		#k1.copy_input_data = []
		#k1.link_input_data = []
		#k1.copy_output_data = []
		#k1.download_output_data = []
		return k1

	def branch_6(self, instance):

		flag = self.get_output(stage=6, instance=instance)
		print 'Output of stage:6, instance:{0} = {1}'.format(instance,flag)
		if  int(flag) < 3:
			self.set_next_stage(1)
			print 'Restarting instance {0}'.format(instance)
		else:
			pass

	

if __name__ == '__main__':

	# Create pattern object with desired ensemble size, pipeline size
	pipe = Test(ensemble_size=2, pipeline_size=1)

	# Create an application manager
	app = AppManager(name='Ebola')

	# Register kernels to be used
	app.register_kernels(echo_kernel)
	#app.register_kernels(rand_kernel)
	app.register_kernels(sleep_kernel)

	# Add workload to the application manager
	app.add_workload(pipe)
	

	# Create a resource handle for target machine
	res = ResourceHandle(resource="local.localhost",
				cores=4,
				#username='vivek91',
				#project = 'TG-MCB090174',
				#queue='development',
				walltime=10,
				database_url='mongodb://entk_user:entk_user@ds029224.mlab.com:29224/entk_doc')

	# Submit request for resources + wait till job becomes Active
	res.allocate(wait=True)

	# Run the given workload
	res.run(app)

	# Deallocate the resource
	res.deallocate()
	
