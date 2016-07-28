__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.entk import PoE, AppManager, Kernel, ResourceHandle, Monitor

#from hello import hello_kernel
from sleep import sleep_kernel
from echo import echo_kernel
from randval import rand_kernel

class Test(PoE):

	def __init__(self, ensemble_size, pipeline_size, iterations):
		super(Test,self).__init__(ensemble_size, pipeline_size, iterations)

	def stage_1(self, instance):
		k1 = Kernel(name="randval")
		k1.arguments = ["--upperlimit=5"]
		k1.cores = 1

		return k1


	def branch_1(self):

		#self.get_file(iteration=1, stage=1, filename="output.txt", instance=1,new_name="temp.txt")
		branch_flag = self.get_output(iteration=1, stage=1, instance=1)
		print branch_flag, type(branch_flag)


	def stage_2(self, instance):
		k1 = Kernel(name="echo")
		k1.arguments = ["--file=output.txt","--text=second_stage"]
		k1.cores = 1

		return k1

	
if __name__ == '__main__':

	pipe = Test(ensemble_size=2, pipeline_size=2, iterations=1)

	app = AppManager(name='firstapp')

	app.register_kernels(rand_kernel)
	app.register_kernels(echo_kernel)
	app.add_workload(pipe)
	
	res = ResourceHandle(resource="local.localhost",
				cores=1,
				#username='vivek91',
				#project = 'TG-MCB090174',
				#queue='development',
				walltime=5,
				database_url='mongodb://entk_user:entk_user@ds029224.mlab.com:29224/entk_doc')
	res.allocate(wait=True)

	res.run(app)

	res.deallocate()
	