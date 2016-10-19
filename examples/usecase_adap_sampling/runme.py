__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.entk import EoP, AppManager, Kernel, ResourceHandle

from echo import echo_kernel
from randval import rand_kernel
from sleep import sleep_kernel

ENSEMBLE_SIZE=2
INPUT_PAR_Q = [20 for x in range(1, ENSEMBLE_SIZE+1)]

def find_tasks(filename):
	pass

class Test(EoP):

	def __init__(self, ensemble_size, pipeline_size):
		super(Test,self).__init__(ensemble_size, pipeline_size)

	def stage_1(self, instance):

		global INPUT_PAR
		global ENSEMBLE_SIZE

		if instance <= ENSEMBLE_SIZE:

			k1 = Kernel(name="sleep")
			k1.arguments = ["--file=output.txt","--text=simulation","--duration={0}".format(INPUT_PAR_Q[instance-1])]
			k1.cores = 1

			# File staging can be added using the following
			#k1.upload_input_data = []
			#k1.copy_input_data = []
			#k1.link_input_data = []
			#k1.copy_output_data = []
			#k1.download_output_data = []

			m1 = Kernel(name="randval", ktype="monitor")
			m1.timeout = 20
			m1.arguments = ["--upperlimit=","--text=monitor"]
			m1.copy_input_data = ['$STAGE_1_TASK_2/output.txt']
			m1.download_output_data = ['output.txt']

			return [k1,m1]


	def branch_1(self, instance):


		# Get the output of the second task of the first stage
		flag = self.get_output(stage=1, task=2)
		print 'Output of stage 1 = {0}'.format(flag)

		# Transfer "output.txt" file from second task of first stage and rename to "kern_data.txt"
		self.get_file(stage=1, task=2, filename="output.txt", new_name="kern_data.txt")
		f = open('kern_data.txt','r')
		print f.read()

		# Transfer "output.txt" file from monitor of first stage and rename to "monitor_data.txt"
		self.get_file(stage=1, monitor=True, filename='output.txt', new_name='monitor_data.txt')
		f = open('monitor_data.txt','r')
		print f.read()

		# Based on a value, one may set the next stage of the workflow. Restart by setting it to 1
		''' 
		if int(flag) >= 3:
			self.set_next_stage(1)
			print 'Restarting workflow'
		else:
			pass
		'''
	

if __name__ == '__main__':

	# Create pattern object with desired ensemble size, pipeline size
	pipe = Test(ensemble_size=ENSEMBLE_SIZE+1, pipeline_size=2)

	# Create an application manager
	app = AppManager(name='MSM')

	# Register kernels to be used
	app.register_kernels(echo_kernel)
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
				database_url='mongodb://rp:rp@ds015335.mlab.com:15335/rp')

	# Submit request for resources + wait till job becomes Active
	res.allocate(wait=True)

	# Run the given workload
	res.run(app)

	# Deallocate the resource
	res.deallocate()
	