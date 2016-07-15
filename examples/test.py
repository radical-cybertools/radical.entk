__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

from radical.entk import PoE, AppManager, Kernel, ResourceHandle

from hello import hello_kernel

class Test(PoE):

	def __init__(self, ensemble_size, pipeline_size, iterations):
		super(Test,self).__init__(ensemble_size, pipeline_size, iterations)

	def stage_1(self, instance):
		k1 = Kernel(name="hello_module")
		k1.arguments = ["--file=test.txt"]
		k1.upload_input_data = ["hello.py"]
		k1.cores = 1
		return k1

	def monitor_1(self):

		pass

	def stage_2(self, instance):
		k1 = Kernel(name="hello_module")
		k1.arguments = ["--file=test.txt"]
		k1.cores = 1
		return k1
	

if __name__ == '__main__':

	pipe = Test(ensemble_size=16, pipeline_size=2, iterations=1)

	app = AppManager(name='firstapp')

	app.register_kernels(hello_kernel)
	app.add_workload(pipe)
	
	res = ResourceHandle(resource="local.localhost",
				cores=1,
	#			username='vivek91',
	#			project = 'TG-MCB090174',
	#			queue='development',
				walltime=5,
				database_url='mongodb://entk_user:entk_user@ds029224.mlab.com:29224/entk_doc')
	res.allocate(wait=True)

	res.run(app)

	res.deallocate()
	