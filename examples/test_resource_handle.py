__author__    = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


from radical.entk import ResourceHandle

if __name__ == "__main__":

	res = ResourceHandle(resource="xsede.stampede",
				cores=1,
				username='vivek91',
				project = 'TG-MCB090174',
				queue='development',
				walltime=5,
				database_url='mongodb://entk_user:entk_user@ds029224.mlab.com:29224/entk_doc')
	res.allocate(wait=True)

	res.deallocate()