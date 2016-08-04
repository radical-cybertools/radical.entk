__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.entk import PoE, AppManager, Kernel, ResourceHandle, Monitor

from echo import echo_kernel
from randval import rand_kernel

class Test(PoE):

	def __init__(self, ensemble_size, pipeline_size):
		super(Test,self).__init__(ensemble_size, pipeline_size)

	def stage_1(self, instance):
		k1 = Kernel(name="echo")
		k1.arguments = ["--file=output.txt","--text=sequence_search"]
		k1.cores = 1

		# File staging
		#k1.upload_input_data = []
		#k1.copy_input_data = []
		#k1.link_input_data = []
		#k1.copy_output_data = []
		#k1.download_output_data = []
		return k1


	def stage_2(self, instance):
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

	def branch_2(self):

		flag = self.get_output(iteration=1, stage=2, instance=1)
		print 'Output of stage 5 = {0}'.format(flag)
		if int(flag) >= 3:
			self.set_next_stage(1)
			print 'Restarting instance {0}'.format(instance)
		else:
			pass
	

if __name__ == '__main__':

	# Create pattern object with desired ensemble size, pipeline size
	pipe = Test(ensemble_size=2, pipeline_size=2)

	# Create an application manager
	app = AppManager(name='MSM')

	# Register kernels to be used
	app.register_kernels(echo_kernel)
	app.register_kernels(rand_kernel)

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
	