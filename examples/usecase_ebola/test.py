__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.entk import EoP, AppManager, Kernel, ResourceHandle, Monitor

from echo import echo_kernel
from randval import rand_kernel

class Test(EoP):

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
		k1 = Kernel(name="echo")
		k1.arguments = ["--file=output.txt","--text=build_systems"]
		k1.copy_input_data=['$STAGE_1/output.txt > temp.txt']
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

		m1 = Monitor(name="yada", timeout=10)
		m1.get_output = ['$INSTANCE_1/temp.txt','$INSTANCE_5/temp.txt']

		return [k1,m1]

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

		branch_flag = self.get_output(iteration=1, stage=5, instance=instance)
		print branch_flag, type(branch_flag)
		if int(branch_flag) >= 3:
			self.set_next_stage(1)
			#self.iteration+=1
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

		branch_flag = self.get_output(iteration=1, stage=6, instance=instance)
		print branch_flag
		if  int(branch_flag) > 4:
			self.set_next_stage(1)
			#self.iteration+=1
		else:
			pass

	

if __name__ == '__main__':

	pipe = Test(ensemble_size=2, pipeline_size=6)

	app = AppManager(name='firstapp')

	app.register_kernels(echo_kernel)
	app.register_kernels(rand_kernel)
	app.add_workload(pipe)
	
	res = ResourceHandle(resource="local.localhost",
				cores=4,
				#username='vivek91',
				#project = 'TG-MCB090174',
				#queue='development',
				walltime=10,
				database_url='mongodb://entk_user:entk_user@ds029224.mlab.com:29224/entk_doc')
	res.allocate(wait=True)

	res.run(app)

	res.deallocate()
	